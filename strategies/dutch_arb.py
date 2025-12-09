import asyncio
from typing import Optional

from poly_data.trading_utils import get_best_bid_ask_deets

from .base import BaseStrategy


class DutchArbStrategy(BaseStrategy):
    """Simple two-leg arbitrage that buys both sides below par.

    The strategy looks for opportunities where the combined best asks for both
    outcomes plus a small buffer are priced below 1. It executes the legs
    sequentially ("legging"), verifies fills, and attempts to merge matched
    positions to realize the arb. If the second leg cannot be completed at a
    profitable price, it unwinds the filled first leg to limit risk.
    """

    async def execute(self, market_id, market_data):
        async with self.get_lock(market_id):
            buffer = float(market_data.get("arb_buffer", 0.005))
            min_size = float(market_data.get("min_size", 0))
            max_size = float(market_data.get("max_size", market_data.get("trade_size", 0)))
            neg_risk = str(market_data.get("neg_risk", "")).upper() == "TRUE"

            token1 = str(market_data["token1"])
            token2 = str(market_data["token2"])

            first_leg = self._get_top_of_book(market_id, "token1", min_size)
            second_leg = self._get_top_of_book(market_id, "token2", min_size)

            if not first_leg or not second_leg:
                return

            ask_sum = first_leg["ask"] + second_leg["ask"] + buffer
            if ask_sum >= 1:
                return

            available_liquidity = min(first_leg["ask_size"], second_leg["ask_size"])
            if available_liquidity <= 0:
                return

            bankroll = self.client.get_usdc_balance()
            max_by_balance = bankroll / max(first_leg["ask"] + second_leg["ask"], 1e-6)
            target_size = min(available_liquidity, max_size or available_liquidity, max_by_balance)

            if target_size < min_size or target_size <= 0:
                return

            _, pre_pos1 = self.client.get_position(token1)
            _, pre_pos2 = self.client.get_position(token2)

            self.client.create_order(token1, "BUY", first_leg["ask"], target_size, neg_risk)
            await asyncio.sleep(0.5)

            post_pos1_raw, post_pos1 = self.client.get_position(token1)
            filled_leg = max(0, post_pos1 - pre_pos1)
            if filled_leg <= 0:
                return

            refreshed_second = self._get_top_of_book(market_id, "token2", min_size)
            if not refreshed_second or refreshed_second["ask"] + first_leg["ask"] + buffer >= 1:
                self._unwind_leg(token1, "token1", filled_leg, market_id, min_size, neg_risk)
                return

            size_second_leg = min(filled_leg, refreshed_second["ask_size"], max_by_balance)
            self.client.create_order(token2, "BUY", refreshed_second["ask"], size_second_leg, neg_risk)
            await asyncio.sleep(0.5)

            post_pos2_raw, post_pos2 = self.client.get_position(token2)
            filled_second = max(0, post_pos2 - pre_pos2)

            if filled_second <= 0:
                self._unwind_leg(token1, "token1", filled_leg, market_id, min_size, neg_risk)
                return

            refreshed_raw1, _ = self.client.get_position(token1)
            refreshed_raw2, _ = self.client.get_position(token2)

            raw1 = refreshed_raw1 if refreshed_raw1 is not None else post_pos1_raw
            raw2 = refreshed_raw2 if refreshed_raw2 is not None else post_pos2_raw

            merge_amount = min(raw1, raw2)
            if merge_amount > 0:
                self.client.merge_positions(merge_amount, market_id, neg_risk)

    def _get_top_of_book(self, market_id: str, name: str, min_size: float) -> Optional[dict]:
        """Fetch best bid/ask snapshot for the specified token view."""

        details = get_best_bid_ask_deets(market_id, name, min_size or 1, 0.05)

        best_bid = details.get("best_bid")
        best_ask = details.get("best_ask")
        best_bid_size = details.get("best_bid_size") or 0
        best_ask_size = details.get("best_ask_size") or 0

        if best_ask is None or best_ask_size <= 0:
            return None

        return {
            "bid": best_bid,
            "ask": best_ask,
            "bid_size": best_bid_size,
            "ask_size": best_ask_size,
        }

    def _unwind_leg(
        self,
        token: str,
        book_name: str,
        size: float,
        market_id: str,
        min_size: float,
        neg_risk: bool,
    ) -> None:
        """Attempt to sell an already filled leg to limit exposure."""

        snapshot = self._get_top_of_book(market_id, book_name, min_size)
        if not snapshot or snapshot["bid"] is None:
            return

        sell_size = min(size, snapshot["bid_size"])
        if sell_size <= 0:
            return

        self.client.create_order(token, "SELL", snapshot["bid"], sell_size, neg_risk)

from typing import Dict, List, Type

import poly_data.global_state as global_state
from strategies.base import BaseStrategy
from strategies.market_maker import MarketMakerStrategy
from strategies.dutch_arb import DutchArbStrategy


class StrategyManager:
    def __init__(
        self,
        registry: Dict[str, Type[BaseStrategy]] | None = None,
    ):
        self.registry = registry or {
            "market_maker": MarketMakerStrategy,
            "dutch_arb": DutchArbStrategy,
        }
        self._instances: Dict[str, BaseStrategy] = {}

    def get_strategy_instance(self, identifier: str) -> BaseStrategy:
        if identifier not in self.registry:
            raise KeyError(f"Strategy {identifier} is not registered")

        if identifier not in self._instances:
            self._instances[identifier] = self.registry[identifier](client=global_state.client)
        return self._instances[identifier]

    def get_strategies_for_market(self, condition_id: str) -> List[BaseStrategy]:
        strategy_ids = global_state.strategy_config.get(str(condition_id), [])
        strategies: List[BaseStrategy] = []

        for identifier in strategy_ids:
            try:
                strategies.append(self.get_strategy_instance(identifier))
            except KeyError as exc:
                print(f"Strategy not found for market {condition_id}: {identifier} ({exc})")

        return strategies

    async def execute_strategies(self, condition_id: str, market_data):
        strategies = self.get_strategies_for_market(condition_id)

        for strategy in strategies:
            try:
                await strategy.execute(condition_id, market_data)
            except Exception as exc:
                print(f"Error executing strategy {strategy.__class__.__name__} for {condition_id}: {exc}")


strategy_manager = StrategyManager()

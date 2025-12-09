import asyncio
from abc import ABC, abstractmethod

import poly_data.global_state as global_state


class BaseStrategy(ABC):
    """Base class for trading strategies.

    Provides shared wiring to the global client and per-market locks to
    prevent concurrent executions on the same market.
    """

    market_locks = {}

    def __init__(self, client=None):
        self.client = client or global_state.client

    def get_lock(self, market_id: str) -> asyncio.Lock:
        if market_id not in self.market_locks:
            self.market_locks[market_id] = asyncio.Lock()
        return self.market_locks[market_id]

    @abstractmethod
    async def execute(self, market_id, market_data):
        """Run the strategy for the given market."""
        raise NotImplementedError

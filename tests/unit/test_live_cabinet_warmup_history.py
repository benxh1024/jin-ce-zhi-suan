import pandas as pd

from src.core.live_cabinet import LiveCabinet
from src.strategies.base_strategy import BaseStrategy


def test_prime_strategy_history_converts_list_container_to_dict():
    strategy = BaseStrategy("X1")
    strategy.history = []
    df = pd.DataFrame([{"code": "002432.SZ", "close": 10.2}])

    LiveCabinet._prime_strategy_history(strategy, "002432.SZ", df)

    assert isinstance(strategy.history, dict)
    assert "002432.SZ" in strategy.history
    assert strategy.history["002432.SZ"].equals(df)
    assert strategy.history["002432.SZ"] is not df


def test_prime_strategy_history_keeps_existing_dict_entries():
    strategy = BaseStrategy("X2")
    strategy.history = {"000001.SZ": pd.DataFrame([{"close": 8.8}])}
    df = pd.DataFrame([{"code": "002432.SZ", "close": 10.5}])

    LiveCabinet._prime_strategy_history(strategy, "002432.SZ", df)

    assert "000001.SZ" in strategy.history
    assert "002432.SZ" in strategy.history
    assert strategy.history["002432.SZ"].equals(df)

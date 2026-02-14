from __future__ import annotations

import datetime as dt

import polars as pl

from basalt.analytics.l2 import L2AnalyticsConfig, L2AnalyticsTW, L2SpreadConfig
from basalt.analytics.trade import TradeAnalytics, TradeAnalyticsConfig, TradeGenericConfig
from basalt.configuration import AnalyticsConfig, PassConfig
from basalt.pipeline import AnalyticsPipeline


def test_daily_analytics_mode_overrides_timebucket_and_vwap_uses_continuous_trading():
    batch_date = "2025-01-07"
    pass_cfg = PassConfig(
        name="pass1",
        modules=["trade"],
        timeline_mode="daily_analytics",
        trade_analytics=TradeAnalyticsConfig(
            generic_metrics=[
                TradeGenericConfig(
                    measures=["VWAP"],
                    sides=["Total"],
                    market_states=["CONTINUOUS_TRADING"],
                )
            ],
            discrepancy_metrics=[],
            flag_metrics=[],
            change_metrics=[],
            impact_metrics=[],
        ),
    )
    config = AnalyticsConfig(PASSES=[pass_cfg], TABLES_TO_LOAD=["trades"])
    pipe = AnalyticsPipeline(
        [TradeAnalytics(pass_cfg.trade_analytics)],
        config,
        pass_cfg,
        context={"__batch_date__": batch_date},
    )

    trades = pl.DataFrame(
        {
            "MIC": ["XPAR", "XPAR", "XPAR"],
            "ListingId": [1, 1, 1],
            "Ticker": ["ABC", "ABC", "ABC"],
            "TimeBucket": [
                dt.datetime(2025, 1, 7, 8, 0),
                dt.datetime(2025, 1, 7, 9, 0),
                dt.datetime(2025, 1, 7, 16, 35),
            ],
            "Classification": ["LIT_CONTINUOUS", "LIT_CONTINUOUS", "LIT_CONTINUOUS"],
            "AggressorSide": [1, 1, 1],
            "MarketState": ["PRE_OPEN", "CONTINUOUS_TRADING", "POST_TRADE"],
            "LocalPrice": [10.0, 20.0, 200.0],
            "Size": [10.0, 30.0, 1000.0],
        }
    )

    out = pipe.run_on_multi_tables(trades=trades)

    expected_eod = dt.datetime(2025, 1, 7, 23, 59, 59, 999999)
    assert out["TimeBucket"][0] == expected_eod
    vwap_col = next(c for c in out.columns if "VWAP" in c)
    assert abs(float(out[vwap_col][0]) - 20.0) < 1e-9


def test_daily_analytics_mode_twap_respects_market_state_filter():
    batch_date = "2025-01-07"
    l2 = pl.DataFrame(
        {
            "MIC": ["XPAR", "XPAR", "XPAR", "XPAR"],
            "ListingId": [1, 1, 1, 1],
            "Ticker": ["ABC", "ABC", "ABC", "ABC"],
            "CurrencyCode": ["EUR", "EUR", "EUR", "EUR"],
            "EventTimestamp": [
                dt.datetime(2025, 1, 7, 8, 0),
                dt.datetime(2025, 1, 7, 9, 0),
                dt.datetime(2025, 1, 7, 10, 0),
                dt.datetime(2025, 1, 7, 16, 35),
            ],
            "TimeBucket": [
                dt.datetime(2025, 1, 7, 8, 0),
                dt.datetime(2025, 1, 7, 9, 0),
                dt.datetime(2025, 1, 7, 10, 0),
                dt.datetime(2025, 1, 7, 16, 35),
            ],
            "MarketState": [
                "PRE_OPEN",
                "CONTINUOUS_TRADING",
                "CONTINUOUS_TRADING",
                "POST_TRADE",
            ],
            "BidPrice1": [10.0, 10.0, 10.0, 10.0],
            "AskPrice1": [40.0, 11.0, 13.0, 80.0],
        }
    )

    filtered_cfg = PassConfig(
        name="pass1",
        modules=["l2tw"],
        timeline_mode="daily_analytics",
        l2_analytics=L2AnalyticsConfig(
            levels=1,
            spreads=[
                L2SpreadConfig(
                    variant=["Abs"],
                    aggregations=["TWA"],
                    market_states=["CONTINUOUS_TRADING"],
                )
            ],
            liquidity=[],
            imbalances=[],
            volatility=[],
            ohlc=[],
        ),
    )
    unfiltered_cfg = PassConfig(
        name="pass1",
        modules=["l2tw"],
        timeline_mode="daily_analytics",
        l2_analytics=L2AnalyticsConfig(
            levels=1,
            spreads=[L2SpreadConfig(variant=["Abs"], aggregations=["TWA"])],
            liquidity=[],
            imbalances=[],
            volatility=[],
            ohlc=[],
        ),
    )

    filtered_pipe = AnalyticsPipeline(
        [L2AnalyticsTW(filtered_cfg.l2_analytics)],
        AnalyticsConfig(PASSES=[filtered_cfg], TABLES_TO_LOAD=["l2"]),
        filtered_cfg,
        context={"__batch_date__": batch_date},
    )
    unfiltered_pipe = AnalyticsPipeline(
        [L2AnalyticsTW(unfiltered_cfg.l2_analytics)],
        AnalyticsConfig(PASSES=[unfiltered_cfg], TABLES_TO_LOAD=["l2"]),
        unfiltered_cfg,
        context={"__batch_date__": batch_date},
    )

    filtered_out = filtered_pipe.run_on_multi_tables(l2=l2)
    unfiltered_out = unfiltered_pipe.run_on_multi_tables(l2=l2)

    filtered_col = next(c for c in filtered_out.columns if c.endswith("SpreadAbsTWA"))
    unfiltered_col = next(c for c in unfiltered_out.columns if c.endswith("SpreadAbsTWA"))

    assert filtered_out["TimeBucket"][0] == dt.datetime(2025, 1, 7, 23, 59, 59, 999999)
    assert float(filtered_out[filtered_col][0]) < float(unfiltered_out[unfiltered_col][0])

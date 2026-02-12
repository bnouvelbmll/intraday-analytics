from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Callable

import polars as pl


@dataclass(frozen=True)
class PlotModule:
    name: str
    description: str
    is_available: Callable[[pl.DataFrame, str | None], bool]
    render: Callable[[pl.DataFrame, str | None], None]


def _find_column(columns: list[str], candidates: list[str]) -> str | None:
    by_lower = {c.lower(): c for c in columns}
    for c in candidates:
        hit = by_lower.get(c.lower())
        if hit:
            return hit
    return None


def _find_first_containing(columns: list[str], words: list[str]) -> str | None:
    for col in columns:
        low = col.lower()
        if all(w in low for w in words):
            return col
    return None


def _ohlc_available(df: pl.DataFrame, time_col: str | None) -> bool:
    cols = df.columns
    needed = [
        _find_column(cols, ["Open", "OpenPrice"]),
        _find_column(cols, ["High", "HighPrice"]),
        _find_column(cols, ["Low", "LowPrice"]),
        _find_column(cols, ["Close", "ClosePrice", "MidPrice"]),
    ]
    return time_col is not None and all(x is not None for x in needed)


def _plot_ohlc(df: pl.DataFrame, time_col: str | None) -> None:
    import streamlit as st
    import pandas as pd
    import plotly.graph_objects as go

    if time_col is None:
        return
    cols = df.columns
    o = _find_column(cols, ["Open", "OpenPrice"])
    h = _find_column(cols, ["High", "HighPrice"])
    l = _find_column(cols, ["Low", "LowPrice"])
    c = _find_column(cols, ["Close", "ClosePrice", "MidPrice"])
    if not all([o, h, l, c]):
        return
    pdf = df.select([time_col, o, h, l, c]).to_pandas()
    pdf[time_col] = pd.to_datetime(pdf[time_col], errors="coerce")
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=pdf[time_col],
                open=pdf[o],
                high=pdf[h],
                low=pdf[l],
                close=pdf[c],
                name="OHLC",
            )
        ]
    )
    fig.update_layout(height=340, margin=dict(l=20, r=20, t=20, b=20))
    st.plotly_chart(fig, use_container_width=True)


def _price_volume_available(df: pl.DataFrame, time_col: str | None) -> bool:
    cols = df.columns
    price = _find_column(cols, ["Close", "ClosePrice", "MidPrice", "Price"])
    volume = _find_column(cols, ["Volume", "TradeVolume", "Qty", "Quantity"])
    return time_col is not None and (price is not None or volume is not None)


def _plot_price_volume(df: pl.DataFrame, time_col: str | None) -> None:
    import streamlit as st
    import pandas as pd
    import plotly.express as px

    if time_col is None:
        return
    cols = df.columns
    price = _find_column(cols, ["Close", "ClosePrice", "MidPrice", "Price"])
    volume = _find_column(cols, ["Volume", "TradeVolume", "Qty", "Quantity"])
    selected = [time_col] + [x for x in [price, volume] if x]
    pdf = df.select(selected).to_pandas()
    pdf[time_col] = pd.to_datetime(pdf[time_col], errors="coerce")
    if price:
        st.plotly_chart(
            px.line(pdf, x=time_col, y=price, title=f"Price: {price}"),
            use_container_width=True,
        )
    if volume:
        st.plotly_chart(
            px.bar(pdf, x=time_col, y=volume, title=f"Volume: {volume}"),
            use_container_width=True,
        )


def _imbalance_available(df: pl.DataFrame, _time_col: str | None) -> bool:
    return any("imbalance" in c.lower() for c in df.columns)


def _plot_imbalance(df: pl.DataFrame, time_col: str | None) -> None:
    import streamlit as st
    import pandas as pd
    import plotly.express as px

    if time_col is None:
        return
    imbalance = [c for c in df.columns if "imbalance" in c.lower()]
    if not imbalance:
        return
    col = imbalance[0]
    pdf = df.select([time_col, col]).to_pandas()
    pdf[time_col] = pd.to_datetime(pdf[time_col], errors="coerce")
    st.plotly_chart(
        px.line(pdf, x=time_col, y=col, title=f"Imbalance: {col}"),
        use_container_width=True,
    )


def _volume_profile_time_available(df: pl.DataFrame, time_col: str | None) -> bool:
    volume = _find_column(df.columns, ["Volume", "TradeVolume", "Qty", "Quantity"])
    return time_col is not None and volume is not None


def _plot_volume_profile_time(df: pl.DataFrame, time_col: str | None) -> None:
    import streamlit as st
    import pandas as pd
    import plotly.express as px

    if time_col is None:
        return
    volume = _find_column(df.columns, ["Volume", "TradeVolume", "Qty", "Quantity"])
    if volume is None:
        return
    pdf = df.select([time_col, volume]).to_pandas()
    pdf[time_col] = pd.to_datetime(pdf[time_col], errors="coerce")
    profile = pdf.groupby(pdf[time_col].dt.floor("5min"), dropna=True)[volume].sum().reset_index()
    st.plotly_chart(
        px.bar(profile, x=time_col, y=volume, title="Volume Profile By Time (5min)"),
        use_container_width=True,
    )


def _volume_profile_price_available(df: pl.DataFrame, _time_col: str | None) -> bool:
    price = _find_column(df.columns, ["Close", "ClosePrice", "MidPrice", "Price"])
    volume = _find_column(df.columns, ["Volume", "TradeVolume", "Qty", "Quantity"])
    return price is not None and volume is not None


def _plot_volume_profile_price(df: pl.DataFrame, _time_col: str | None) -> None:
    import streamlit as st
    import plotly.express as px

    price = _find_column(df.columns, ["Close", "ClosePrice", "MidPrice", "Price"])
    volume = _find_column(df.columns, ["Volume", "TradeVolume", "Qty", "Quantity"])
    if price is None or volume is None:
        return
    pdf = df.select([price, volume]).to_pandas()
    if pdf.empty:
        return
    st.plotly_chart(
        px.histogram(
            pdf,
            x=price,
            y=volume,
            nbins=40,
            histfunc="sum",
            title="Volume Profile By Price",
        ),
        use_container_width=True,
    )


def _aggressive_trade_available(df: pl.DataFrame, _time_col: str | None) -> bool:
    col = _find_first_containing(df.columns, ["aggressive", "size"])
    return col is not None


def _plot_aggressive_trade(df: pl.DataFrame, _time_col: str | None) -> None:
    import streamlit as st
    import plotly.express as px

    col = _find_first_containing(df.columns, ["aggressive", "size"])
    if col is None:
        return
    pdf = df.select([col]).to_pandas()
    st.plotly_chart(
        px.histogram(pdf, x=col, nbins=50, title=f"Aggressive Trade Size Distribution ({col})"),
        use_container_width=True,
    )


def _lob_available(df: pl.DataFrame, time_col: str | None) -> bool:
    if time_col is None:
        return False
    bid = _find_column(df.columns, ["BidPrice1", "BestBidPrice"])
    ask = _find_column(df.columns, ["AskPrice1", "BestAskPrice"])
    if bid is not None and ask is not None:
        return True
    bid_qty_cols = [c for c in df.columns if re.match(r"(?i)^bid(quantity|size)\d+$", c)]
    ask_qty_cols = [c for c in df.columns if re.match(r"(?i)^ask(quantity|size)\d+$", c)]
    return bool(bid_qty_cols and ask_qty_cols)


def _plot_lob(df: pl.DataFrame, time_col: str | None) -> None:
    import streamlit as st
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go

    if time_col is None:
        return
    bid = _find_column(df.columns, ["BidPrice1", "BestBidPrice"])
    ask = _find_column(df.columns, ["AskPrice1", "BestAskPrice"])
    bq = _find_column(df.columns, ["BidQuantity1", "BestBidQty", "BidSize1"])
    aq = _find_column(df.columns, ["AskQuantity1", "BestAskQty", "AskSize1"])

    top, depth = st.tabs(["Top Of Book", "Depth Viewer"])
    if bid is not None and ask is not None:
        selected = [time_col, bid, ask]
        if bq:
            selected.append(bq)
        if aq:
            selected.append(aq)
        pdf = df.select(selected).to_pandas()
        pdf[time_col] = pd.to_datetime(pdf[time_col], errors="coerce")
        pdf["Spread"] = pdf[ask] - pdf[bid]
        with top:
            st.plotly_chart(
                px.line(pdf, x=time_col, y="Spread", title="L1 Spread"),
                use_container_width=True,
            )
            if bq and aq:
                pdf["DepthImbalance"] = (pdf[bq] - pdf[aq]) / (pdf[bq] + pdf[aq]).replace(0, 1)
                st.plotly_chart(
                    px.line(pdf, x=time_col, y="DepthImbalance", title="L1 Depth Imbalance"),
                    use_container_width=True,
                )
    else:
        with top:
            st.info("Top-of-book price columns not found. Showing depth view only.")

    def _extract_depth_cols(prefix: str) -> list[tuple[int, str]]:
        cols: list[tuple[int, str]] = []
        for col in df.columns:
            m = re.match(rf"(?i)^{prefix}(quantity|size)(\d+)$", col)
            if m:
                cols.append((int(m.group(2)), col))
        cols.sort(key=lambda x: x[0])
        return cols

    bid_depth = _extract_depth_cols("bid")
    ask_depth = _extract_depth_cols("ask")
    if not bid_depth or not ask_depth:
        with depth:
            st.info("Depth columns like BidQuantity1/AskQuantity1 were not detected.")
        return

    max_levels = min(len(bid_depth), len(ask_depth), 20)
    with depth:
        levels = st.slider("Levels", min_value=1, max_value=max_levels, value=min(10, max_levels))
        stride = st.slider("Time Downsample Stride", min_value=1, max_value=20, value=3)
        bid_cols = [c for _lvl, c in bid_depth[:levels]]
        ask_cols = [c for _lvl, c in ask_depth[:levels]]
        sample = df.select([time_col] + bid_cols + ask_cols)
        sample = sample[::stride]
        pdf = sample.to_pandas()
        pdf[time_col] = pd.to_datetime(pdf[time_col], errors="coerce")
        bid_mat = pdf[bid_cols].to_numpy().T
        ask_mat = pdf[ask_cols].to_numpy().T
        fig = go.Figure()
        fig.add_trace(
            go.Heatmap(
                z=bid_mat,
                x=pdf[time_col],
                y=[f"BidL{i}" for i in range(1, levels + 1)],
                colorscale="Blues",
                name="BidDepth",
                colorbar=dict(title="BidQty"),
            )
        )
        fig.add_trace(
            go.Heatmap(
                z=ask_mat,
                x=pdf[time_col],
                y=[f"AskL{i}" for i in range(1, levels + 1)],
                colorscale="Reds",
                name="AskDepth",
                opacity=0.6,
                colorbar=dict(title="AskQty", x=1.05),
            )
        )
        fig.update_layout(height=450, title="LOB Depth Heatmap", margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Inspired by lobv: level-based view with pluggable panel integration.")


def default_plot_modules() -> list[PlotModule]:
    return [
        PlotModule(
            name="OHLC Bars",
            description="Candlestick chart when OHLC columns are present.",
            is_available=_ohlc_available,
            render=_plot_ohlc,
        ),
        PlotModule(
            name="Price And Volume",
            description="Basic price/volume overview.",
            is_available=_price_volume_available,
            render=_plot_price_volume,
        ),
        PlotModule(
            name="Volume Imbalance",
            description="First imbalance-like series.",
            is_available=_imbalance_available,
            render=_plot_imbalance,
        ),
        PlotModule(
            name="Volume Profile By Time",
            description="5-minute grouped volume profile.",
            is_available=_volume_profile_time_available,
            render=_plot_volume_profile_time,
        ),
        PlotModule(
            name="Volume Profile By Price",
            description="Volume histogram weighted by price.",
            is_available=_volume_profile_price_available,
            render=_plot_volume_profile_price,
        ),
        PlotModule(
            name="Aggressive Trade Size",
            description="Distribution of aggressive trade size columns.",
            is_available=_aggressive_trade_available,
            render=_plot_aggressive_trade,
        ),
        PlotModule(
            name="LOB Viewer",
            description="L1 spread/depth microstructure view.",
            is_available=_lob_available,
            render=_plot_lob,
        ),
    ]

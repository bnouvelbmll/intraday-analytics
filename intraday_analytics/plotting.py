from __future__ import annotations

import datetime as dt
from typing import Iterable, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import ipywidgets as widgets

from dagster import AssetKey, DagsterInstance, DagsterEventType, EventRecordsFilter


def _coerce_date(value) -> Optional[dt.date]:
    if value is None:
        return None
    if isinstance(value, dt.date):
        return value
    if isinstance(value, dt.datetime):
        return value.date()
    try:
        return dt.date.fromisoformat(str(value))
    except Exception:
        return None


def _parse_partition_key(
    partition: str,
    date_dim: str = "date",
    universe_dim: str = "universe",
) -> dict:
    if not partition:
        return {}
    parts = partition.split("|")
    out: dict[str, str] = {}
    if all("=" in p for p in parts):
        for p in parts:
            k, v = p.split("=", 1)
            out[k] = v
        return out
    if len(parts) >= 1:
        out[date_dim] = parts[0]
    if len(parts) >= 2:
        out[universe_dim] = parts[1]
    return out


def _metadata_value_to_python(value):
    if value is None:
        return None
    if hasattr(value, "value"):
        return value.value
    return value


def _event_to_row(
    event,
    *,
    metric_key: str,
    date_dim: str,
    universe_dim: str,
) -> Optional[dict]:
    if not event or not event.dagster_event:
        return None
    mat = event.dagster_event.step_materialization_data.materialization
    partition = mat.partition
    parts = _parse_partition_key(partition, date_dim=date_dim, universe_dim=universe_dim)
    date_value = _coerce_date(parts.get(date_dim))
    universe_value = parts.get(universe_dim)
    metadata_value = _metadata_value_to_python(mat.metadata.get(metric_key)) if mat.metadata else None
    return {
        "asset_key": mat.asset_key.to_string(),
        "partition": partition,
        "date": date_value,
        "universe": universe_value,
        "metric": metadata_value,
        "timestamp": event.timestamp,
        "metadata": mat.metadata,
    }


def load_materialization_frame(
    instance: DagsterInstance,
    asset_keys: Iterable[AssetKey],
    *,
    metric_key: str = "size_bytes",
    date_dim: str = "date",
    universe_dim: str = "universe",
    limit: Optional[int] = None,
) -> pd.DataFrame:
    rows = []
    for asset_key in asset_keys:
        records = instance.get_event_records(
            EventRecordsFilter(DagsterEventType.ASSET_MATERIALIZATION, asset_key=asset_key),
            limit=limit,
        )
        for record in records:
            row = _event_to_row(
                record.event_log_entry,
                metric_key=metric_key,
                date_dim=date_dim,
                universe_dim=universe_dim,
            )
            if row:
                rows.append(row)
    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df["metric"] = pd.to_numeric(df["metric"], errors="coerce")
    return df


def _sort_universes(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return []
    first_dates = (
        df.groupby("universe")["date"]
        .min()
        .reset_index()
        .sort_values(["date", "universe"])
    )
    return first_dates["universe"].tolist()


def _apply_filters(
    df: pd.DataFrame,
    *,
    exclude_unpopulated: bool,
):
    if df.empty:
        return df
    if exclude_unpopulated:
        df = df[df["metric"].notna()]
    return df


def calendar_heatmap(
    df: pd.DataFrame,
    *,
    value_col: str,
    title: str,
    color_scale: str = "Viridis",
) -> go.Figure:
    if df.empty:
        return go.Figure()
    tmp = df.copy()
    tmp["date"] = pd.to_datetime(tmp["date"])
    tmp["week_start"] = tmp["date"] - pd.to_timedelta(tmp["date"].dt.weekday, unit="D")
    tmp["dow"] = tmp["date"].dt.weekday
    grid = (
        tmp.groupby(["week_start", "dow"])[value_col]
        .sum()
        .reset_index()
    )
    pivot = grid.pivot(index="dow", columns="week_start", values=value_col)
    fig = px.imshow(
        pivot,
        aspect="auto",
        color_continuous_scale=color_scale,
        labels=dict(x="Week", y="Day of Week", color=value_col),
        title=title,
    )
    return fig


def universe_heatmap(
    df: pd.DataFrame,
    *,
    value_col: str,
    title: str,
    exclude_unpopulated: bool = True,
    color_scale: str = "Viridis",
) -> go.Figure:
    if df.empty:
        return go.Figure()
    df = _apply_filters(df, exclude_unpopulated=exclude_unpopulated)
    order = _sort_universes(df)
    pivot = (
        df.groupby(["universe", "date"])[value_col]
        .sum()
        .reset_index()
        .pivot(index="universe", columns="date", values=value_col)
        .reindex(order)
    )
    fig = px.imshow(
        pivot,
        aspect="auto",
        color_continuous_scale=color_scale,
        labels=dict(x="Date", y="Universe", color=value_col),
        title=title,
    )
    return fig


def build_plots(
    df: pd.DataFrame,
    *,
    metric_label: str = "size_bytes",
    exclude_unpopulated: bool = True,
) -> dict[str, go.Figure]:
    if df.empty:
        return {"calendar_count": go.Figure(), "calendar_size": go.Figure(), "heatmap": go.Figure()}

    count_df = df.groupby("date")["universe"].nunique().reset_index(name="count")
    size_df = df.groupby("date")["metric"].sum().reset_index(name="total")

    return {
        "calendar_count": calendar_heatmap(
            count_df, value_col="count", title="Universe count by date"
        ),
        "calendar_size": calendar_heatmap(
            size_df, value_col="total", title=f"Total {metric_label} by date"
        ),
        "heatmap": universe_heatmap(
            df, value_col="metric", title=f"{metric_label} by universe/date",
            exclude_unpopulated=exclude_unpopulated
        ),
    }


def materialization_dashboard(
    instance: DagsterInstance,
    asset_keys: Iterable[AssetKey],
    *,
    metric_key: str = "size_bytes",
    metric_label: Optional[str] = None,
    date_dim: str = "date",
    universe_dim: str = "universe",
    exclude_unpopulated: bool = True,
    limit: Optional[int] = None,
):
    if metric_label is None:
        metric_label = metric_key

    out = widgets.Output()

    def _render():
        df = load_materialization_frame(
            instance,
            asset_keys,
            metric_key=metric_key,
            date_dim=date_dim,
            universe_dim=universe_dim,
            limit=limit,
        )
        figs = build_plots(
            df,
            metric_label=metric_label,
            exclude_unpopulated=exclude_unpopulated,
        )
        with out:
            out.clear_output(wait=True)
            for fig in figs.values():
                fig.show()

    refresh = widgets.Button(description="Refresh")
    refresh.on_click(lambda _btn: _render())
    _render()
    return widgets.VBox([refresh, out])

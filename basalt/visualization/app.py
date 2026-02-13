from __future__ import annotations

import argparse
import json
import os
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

from basalt.visualization.data import (
    derive_input_table_options,
    filter_frame,
    infer_dataset_options,
    load_input_table_frame,
    load_dataset_frame,
    load_resolved_user_config,
)
from basalt.visualization.modules import discover_plot_modules
from basalt.visualization.scoring import score_numeric_columns, top_interesting_columns


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--pipeline", required=True)
    parser.add_argument("--config_precedence", default="yaml_overrides")
    known, _unknown = parser.parse_known_args(sys.argv[1:])
    return known


def _format_score_table(df_scores) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "column": s.column,
                "entropy": round(s.entropy_score, 4),
                "variance": round(s.variance_score, 4),
                "non_null_ratio": round(s.non_null_ratio, 4),
                "total": round(s.total_score, 4),
            }
            for s in df_scores
        ]
    )


def main() -> None:
    args = _parse_args()
    st.set_page_config(page_title="BMLL Basalt Visualization", layout="wide")
    st.title("BMLL Basalt Visualization Explorer")
    st.caption("Config-driven exploration for wide analytics datasets.")

    auth_mode = str(os.environ.get("UI_PASSWORD_ENABLED", "false")).lower()
    if auth_mode == "password":
        expected = os.environ.get("ACCESS_PASSWORD", "")
        if not expected:
            st.error("ACCESS_PASSWORD is required when UI_PASSWORD_ENABLED=password.")
            return

        def _check_password() -> None:
            entered = st.session_state.get("viz_password", "")
            st.session_state["viz_password_ok"] = entered == expected

        if not st.session_state.get("viz_password_ok", False):
            st.subheader("Authentication Required")
            st.text_input(
                "Password",
                type="password",
                key="viz_password",
                on_change=_check_password,
            )
            if "viz_password_ok" in st.session_state and not st.session_state["viz_password_ok"]:
                st.error("Invalid password.")
            st.stop()

    try:
        config = load_resolved_user_config(
            pipeline=args.pipeline,
            config_precedence=args.config_precedence,
        )
    except Exception as exc:
        st.error(f"Failed to load config: {exc}")
        return

    options = infer_dataset_options(config)
    input_options = derive_input_table_options(config)
    source_mode = st.sidebar.radio(
        "Data Source",
        options=["Pass Outputs", "Input Tables"],
        index=0,
    )

    frame = None
    selected_meta = {}

    with st.sidebar:
        st.subheader("Filters")
        cfg_start = str(config.get("START_DATE") or "")
        cfg_end = str(config.get("END_DATE") or cfg_start)
        if cfg_start:
            default_start = pd.Timestamp(cfg_start).date()
            default_end = pd.Timestamp(cfg_end).date()
            date_range = st.date_input("Date range", value=(default_start, default_end))
            if isinstance(date_range, tuple) and len(date_range) == 2:
                date_start = str(date_range[0])
                date_end = str(date_range[1])
            else:
                date_start = str(date_range)
                date_end = str(date_range)
        else:
            date_start = None
            date_end = None
        instrument_value = None

    if source_mode == "Pass Outputs":
        if not options:
            st.warning("No pass outputs found in config.")
            return
        labels = [f"{o.pass_name} :: {o.path}" for o in options]
        default_idx = next((i for i, o in enumerate(options) if o.is_default), len(options) - 1)
        selected_idx = st.sidebar.selectbox("Dataset (pass output)", range(len(options)), index=default_idx, format_func=lambda i: labels[i])
        selected = options[selected_idx]
        selected_meta = {"pass": selected.pass_name, "path": selected.path, "kind": "output"}
        st.sidebar.code(selected.path)
        try:
            frame = load_dataset_frame(selected.path)
        except Exception as exc:
            st.error(f"Failed to read output dataset: {exc}")
            return
    else:
        if not input_options:
            st.warning("No input tables inferred from config/passes.")
            return
        labels = [
            f"{row.table_name} (passes: {', '.join(row.from_passes) if row.from_passes else 'explicit/unknown'})"
            for row in input_options
        ]
        selected_idx = st.sidebar.selectbox(
            "Input table",
            range(len(input_options)),
            index=0,
            format_func=lambda i: labels[i],
        )
        selected_input = input_options[selected_idx]
        source_candidates = [str(x) for x in config.get("DATA_SOURCE_PATH") or ["bmll"]]
        source = st.sidebar.selectbox("Input source mechanism", source_candidates, index=0)
        if not date_start or not date_end:
            st.error("START_DATE/END_DATE (or date range) are required for input table mode.")
            return
        selected_meta = {
            "table": selected_input.table_name,
            "source": source,
            "kind": "input",
        }
        try:
            frame = load_input_table_frame(
                pipeline=args.pipeline,
                config=config,
                table_name=selected_input.table_name,
                date_start=date_start,
                date_end=date_end,
                source=source,
            )
        except Exception as exc:
            st.error(f"Failed to load input table: {exc}")
            return

    if frame is None or frame.is_empty():
        st.warning("Selected dataset is empty.")
        return

    instrument_col = next((c for c in ["ListingId", "InstrumentId", "PrimaryListingId", "BMLL_OBJECT_ID", "Ticker", "Symbol"] if c in frame.columns), None)
    if instrument_col is not None:
        values = frame[instrument_col].cast(str).unique().sort().to_list()
        instrument_value = st.sidebar.selectbox("Instrument", values, index=0)
    time_col = next((c for c in ["TimeBucket", "Timestamp", "DateTime", "Date", "EventTime"] if c in frame.columns), None)

    filtered, used_time_col, used_instrument_col = filter_frame(
        frame,
        date_start=date_start,
        date_end=date_end,
        instrument_value=instrument_value,
    )

    st.subheader("Selection Summary")
    summary = {
        "rows": filtered.height,
        "columns": filtered.width,
        "time_column": used_time_col,
        "instrument_column": used_instrument_col,
        "date_start": date_start,
        "date_end": date_end,
    }
    summary.update(selected_meta)
    st.write(summary)
    with st.expander("Resolved Config", expanded=False):
        st.code(json.dumps(config, indent=2, default=str))

    if filtered.is_empty():
        st.warning("No rows after filters.")
        return

    st.subheader("Interesting Series (Entropy/Variation Ranking)")
    scored = score_numeric_columns(filtered)
    top = top_interesting_columns(filtered, limit=12)
    selected_series = st.multiselect(
        "Series to display",
        options=[s.column for s in scored],
        default=top,
    )
    if selected_series and used_time_col is not None:
        pdf = filtered.select([used_time_col] + selected_series).to_pandas()
        pdf[used_time_col] = pd.to_datetime(pdf[used_time_col], errors="coerce")
        st.plotly_chart(
            px.line(pdf, x=used_time_col, y=selected_series, title="Top Ranked Series"),
            use_container_width=True,
        )
    st.dataframe(_format_score_table(scored[:30]), use_container_width=True, hide_index=True)

    st.subheader("Modular Finance Plots")
    modules = discover_plot_modules()
    available = [m for m in modules if m.is_available(filtered, used_time_col)]
    if not available:
        st.info("No plot module matched this dataset schema.")
        return
    for module in available:
        with st.expander(f"{module.name} - {module.description}", expanded=True):
            module.render(filtered, used_time_col)


if __name__ == "__main__":
    main()

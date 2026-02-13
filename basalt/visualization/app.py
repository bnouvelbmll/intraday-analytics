from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time

import pandas as pd
import plotly.express as px
import polars as pl
import streamlit as st

from basalt.visualization.background import BackgroundFrameLoader
from basalt.visualization.crossfilter import (
    RangeConstraint,
    SetConstraint,
    apply_constraints,
    candidate_dimensions,
    constraints_to_predicates,
    constraints_to_mongo,
    is_discrete_column,
)
from basalt.visualization.data import (
    available_universe_keys,
    build_load_payload,
    derive_input_table_options,
    filter_frame,
    infer_dataset_options,
    load_frame_from_payload,
    load_reference_snapshot,
    load_resolved_user_config,
    select_subuniverse,
    estimate_listing_days,
)
from basalt.visualization.modules import discover_plot_modules
from basalt.visualization.scales import choose_scale, transform_series
from basalt.visualization.scoring import (
    score_numeric_columns,
    suggest_feature_target_associations,
    top_interesting_columns,
)


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


def _payload_request_id(payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _ensure_bg_loader() -> BackgroundFrameLoader:
    loader = st.session_state.get("_viz_bg_loader")
    if loader is None:
        loader = BackgroundFrameLoader(load_frame_from_payload)
        st.session_state["_viz_bg_loader"] = loader
    return loader


def _initial_panel(df_cols: list[str], time_col: str | None) -> dict:
    numeric = [c for c in df_cols]
    if time_col and numeric:
        target = next((c for c in numeric if c != time_col), numeric[0])
        return {
            "id": 1,
            "dims": 1,
            "columns": [target],
            "plot_type": "historical",
            "as_filter": False,
            "scales": ["auto"],
        }
    target = numeric[0] if numeric else (df_cols[0] if df_cols else "")
    return {
        "id": 1,
        "dims": 1,
        "columns": [target] if target else [],
        "plot_type": "distribution",
        "as_filter": False,
        "scales": ["auto"],
    }


def _plot_type_options(dims: int) -> list[str]:
    if dims == 1:
        return ["distribution", "historical"]
    if dims == 2:
        return ["scatter", "2d-elevation map", "2d-distribution view"]
    return ["scatter3d"]


def _apply_scaled_columns(
    pdf: pd.DataFrame,
    columns: list[str],
    scale_options: list[str],
) -> tuple[pd.DataFrame, list[str], dict[str, str]]:
    out = pdf.copy()
    rendered: list[str] = []
    labels: dict[str, str] = {}
    for idx, col in enumerate(columns):
        if col not in out.columns:
            continue
        scale_opt = str(scale_options[idx] if idx < len(scale_options) else "auto")
        numeric = pd.to_numeric(out[col], errors="coerce")
        if numeric.notna().sum() == 0:
            rendered.append(col)
            labels[col] = "linear"
            continue
        if scale_opt == "auto":
            decision = choose_scale(numeric)
            scale_name = decision.selected
        else:
            scale_name = scale_opt
        transformed = transform_series(numeric, scale_name) if scale_name != "linear" else numeric
        out_col = col if scale_name == "linear" else f"{col} [{scale_name}]"
        out[out_col] = transformed
        rendered.append(out_col)
        labels[col] = scale_name
    return out, rendered, labels


def _render_panel_plot(
    df: pd.DataFrame,
    *,
    panel_id: int,
    dims: int,
    columns: list[str],
    scale_options: list[str],
    plot_type: str,
    time_col: str | None,
) -> None:
    title = f"Panel {panel_id}: {plot_type}"
    if not columns:
        st.info("Select dimensions/columns for this panel.")
        return
    plotted_df, cols, label_map = _apply_scaled_columns(df, columns, scale_options)
    if len(cols) < dims:
        st.info("Selected dimensions are not available.")
        return
    if label_map:
        st.caption(
            "Scales: " + ", ".join([f"{k}={v}" for k, v in label_map.items()])
        )
    if dims == 1:
        c1 = cols[0]
        if c1 not in plotted_df.columns:
            st.info(f"Column `{c1}` not available.")
            return
        if plot_type == "historical":
            x = time_col if time_col in plotted_df.columns else None
            if x is None:
                st.info("Historical plot needs a time column in data.")
                return
            st.plotly_chart(px.line(plotted_df, x=x, y=c1, title=title), use_container_width=True)
            return
        st.plotly_chart(px.histogram(plotted_df, x=c1, nbins=80, title=title), use_container_width=True)
        return

    if dims == 2:
        c1, c2 = cols[:2]
        if c1 not in plotted_df.columns or c2 not in plotted_df.columns:
            st.info("Selected 2D columns are not available.")
            return
        if plot_type == "2d-elevation map":
            st.plotly_chart(
                px.density_heatmap(plotted_df, x=c1, y=c2, nbinsx=60, nbinsy=60, title=title),
                use_container_width=True,
            )
            return
        if plot_type == "2d-distribution view":
            st.plotly_chart(
                px.density_contour(plotted_df, x=c1, y=c2, title=title),
                use_container_width=True,
            )
            return
        st.plotly_chart(px.scatter(plotted_df, x=c1, y=c2, title=title), use_container_width=True)
        return

    c1, c2, c3 = cols[:3]
    if c1 not in plotted_df.columns or c2 not in plotted_df.columns or c3 not in plotted_df.columns:
        st.info("Selected 3D columns are not available.")
        return
    st.plotly_chart(px.scatter_3d(plotted_df, x=c1, y=c2, z=c3, title=title), use_container_width=True)


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
    loader = _ensure_bg_loader()

    selected_meta = {}
    selected_ref = None
    date_start = None
    date_end = None
    source_mode = "Pass Outputs"
    selected_dataset_path = None
    selected_table_name = None
    selected_source = None

    ref_snapshot = None
    with st.sidebar:
        st.subheader("Universe & Date")
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
            date_start, date_end = None, None

        if date_start:
            try:
                ref_snapshot = load_reference_snapshot(pipeline=args.pipeline, date=date_start)
            except Exception as exc:
                st.error(f"Failed to load universe reference: {exc}")
                return
        else:
            ref_snapshot = None

        universe_mode = st.selectbox(
            "Universe selector",
            options=[
                "ISIN (default)",
                "ListingId",
                "InstrumentId",
                "Arbitrary subset",
                "Whole universe",
            ],
            index=0,
        )
        single_value = None
        subset_values: list[str] = []
        subset_column = None
        if ref_snapshot is not None and not ref_snapshot.is_empty():
            keys = available_universe_keys(ref_snapshot)
            if universe_mode in {"ISIN (default)", "ListingId", "InstrumentId"}:
                default_col = universe_mode.split(" ")[0]
                matching = next((k for k in keys if k.lower() == default_col.lower()), None)
                if matching:
                    vals = (
                        ref_snapshot.get_column(matching)
                        .drop_nulls()
                        .cast(str)
                        .unique()
                        .sort()
                        .to_list()
                    )
                    if vals:
                        single_value = st.selectbox(f"{matching} value", vals, index=0)
                else:
                    st.warning(f"{default_col} is not available in reference.")
            elif universe_mode == "Arbitrary subset":
                if not keys:
                    st.warning("No universe key available for subset selection.")
                else:
                    default_idx = next((i for i, k in enumerate(keys) if k.lower() == "isin"), 0)
                    subset_column = st.selectbox("Subset column", keys, index=default_idx)
                    raw_subset = st.text_area(
                        "Subset values (comma/newline separated)",
                        value="",
                        height=96,
                    )
                    subset_values = [
                        v.strip()
                        for chunk in raw_subset.splitlines()
                        for v in chunk.split(",")
                        if v.strip()
                    ]
            selected_ref = select_subuniverse(
                ref_snapshot,
                mode=universe_mode,
                single_value=single_value,
                subset_values=subset_values,
                subset_column=subset_column,
            )
            ld = estimate_listing_days(
                selected_ref,
                date_start=date_start,
                date_end=date_end,
            )
            st.caption(
                f"Estimated Listing-Days (LD): {ld:,} | Selected listings: {selected_ref.height:,}"
            )
        else:
            selected_ref = ref_snapshot

        st.subheader("Data Source")
        source_mode = st.radio(
            "Data Source",
            options=["Pass Outputs", "Input Tables"],
            index=0,
        )

    if source_mode == "Pass Outputs":
        if not options:
            st.warning("No pass outputs found in config.")
            return
        labels = [f"{o.pass_name} :: {o.path}" for o in options]
        default_idx = next((i for i, o in enumerate(options) if o.is_default), len(options) - 1)
        selected_idx = st.sidebar.selectbox("Dataset (pass output)", range(len(options)), index=default_idx, format_func=lambda i: labels[i])
        selected = options[selected_idx]
        selected_dataset_path = selected.path
        selected_meta = {"pass": selected.pass_name, "path": selected.path, "kind": "output"}
        st.sidebar.code(selected.path)
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
        selected_source = source
        selected_table_name = selected_input.table_name
        if not date_start or not date_end:
            st.error("START_DATE/END_DATE (or date range) are required for input table mode.")
            return
        selected_meta = {
            "table": selected_input.table_name,
            "source": source,
            "kind": "input",
        }

    payload = build_load_payload(
        source_mode=source_mode,
        pipeline=args.pipeline,
        config=config,
        dataset_path=selected_dataset_path,
        table_name=selected_table_name,
        source=selected_source,
        date_start=date_start,
        date_end=date_end,
        selected_ref=selected_ref,
    )
    request_id = _payload_request_id(payload)
    if loader.request_id != request_id:
        loader.submit(request_id, payload)

    frame = None
    result = loader.poll()
    if result is not None and result.request_id == request_id:
        if result.status == "error":
            st.error(f"Failed to load data:\n{result.error}")
            return
        frame = loader.consume_dataframe()
        if frame is not None:
            st.session_state["_viz_last_frame"] = frame
            st.session_state["_viz_last_frame_request"] = request_id

    if frame is None and st.session_state.get("_viz_last_frame_request") == request_id:
        frame = st.session_state.get("_viz_last_frame")

    if frame is None:
        st.info("Loading selected dataset in background...")
        time.sleep(0.4)
        st.rerun()
        return

    if frame.is_empty():
        st.warning("Selected dataset is empty.")
        return

    instrument_col = next((c for c in ["ListingId", "InstrumentId", "PrimaryListingId", "BMLL_OBJECT_ID", "Ticker", "Symbol"] if c in frame.columns), None)
    instrument_value = None
    if instrument_col is not None:
        values = frame[instrument_col].cast(str).unique().sort().to_list()
        instrument_value = st.sidebar.selectbox("Instrument", values, index=0)
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

    st.subheader("Crossfilter Panels")
    dims_candidates = candidate_dimensions(filtered)
    if "_viz_panels" not in st.session_state:
        st.session_state["_viz_panels"] = [_initial_panel(dims_candidates, used_time_col)]
        st.session_state["_viz_panel_seq"] = 2

    toolbar = st.columns([1, 1, 2])
    with toolbar[0]:
        if st.button("Add view panel", key="add_panel"):
            nxt = int(st.session_state.get("_viz_panel_seq", 1))
            st.session_state["_viz_panels"].append(
                {
                    "id": nxt,
                    "dims": 1,
                    "columns": [dims_candidates[0]] if dims_candidates else [],
                    "plot_type": "distribution",
                    "as_filter": False,
                    "scales": ["auto"],
                }
            )
            st.session_state["_viz_panel_seq"] = nxt + 1
            st.rerun()
    with toolbar[1]:
        if st.button("Clear filters", key="clear_xfilters"):
            st.session_state["_viz_global_constraints"] = {}
            st.rerun()

    constraints: dict[str, RangeConstraint | SetConstraint] = {}
    remove_ids: list[int] = []
    panels = list(st.session_state.get("_viz_panels", []))
    for i, panel in enumerate(panels):
        pid = int(panel.get("id", i + 1))
        with st.container(border=True):
            head = st.columns([2, 2, 2, 1, 1])
            with head[0]:
                dims = int(
                    st.selectbox(
                        f"View dimensions #{pid}",
                        [1, 2, 3],
                        index=max(0, min(2, int(panel.get("dims", 1)) - 1)),
                        key=f"panel_dims_{pid}",
                    )
                )
            with head[1]:
                ptypes = _plot_type_options(dims)
                default_plot = str(panel.get("plot_type", ptypes[0]))
                plot_type = st.selectbox(
                    f"Plot type #{pid}",
                    ptypes,
                    index=ptypes.index(default_plot) if default_plot in ptypes else 0,
                    key=f"panel_plot_{pid}",
                )
            with head[2]:
                as_filter = bool(
                    st.checkbox(
                        "Use as filter control",
                        value=bool(panel.get("as_filter", False)),
                        key=f"panel_filter_{pid}",
                    )
                )
            with head[3]:
                st.caption(f"Panel {pid}")
            with head[4]:
                if st.button("Remove", key=f"panel_remove_{pid}"):
                    remove_ids.append(pid)

            selected_cols: list[str] = []
            selected_scales: list[str] = []
            for cidx in range(dims):
                default_cols = panel.get("columns", [])
                default_col = default_cols[cidx] if cidx < len(default_cols) else None
                col_name = st.selectbox(
                    f"Column {cidx + 1} #{pid}",
                    dims_candidates,
                    index=dims_candidates.index(default_col) if default_col in dims_candidates else min(cidx, max(0, len(dims_candidates) - 1)),
                    key=f"panel_col_{pid}_{cidx}",
                )
                selected_cols.append(col_name)
                default_scales = panel.get("scales", [])
                default_scale = (
                    default_scales[cidx] if cidx < len(default_scales) else "auto"
                )
                scale_name = st.selectbox(
                    f"Scale {cidx + 1} #{pid}",
                    ["auto", "linear", "sqrt", "sgnlog", "percentile"],
                    index=["auto", "linear", "sqrt", "sgnlog", "percentile"].index(default_scale)
                    if default_scale in {"auto", "linear", "sqrt", "sgnlog", "percentile"}
                    else 0,
                    key=f"panel_scale_{pid}_{cidx}",
                )
                selected_scales.append(scale_name)

            panel["dims"] = dims
            panel["plot_type"] = plot_type
            panel["as_filter"] = as_filter
            panel["columns"] = selected_cols
            panel["scales"] = selected_scales
            panels[i] = panel

            if as_filter and plot_type in {"distribution", "2d-distribution view"}:
                st.caption("Control filter values for this view")
                for c in selected_cols:
                    if c not in filtered.columns:
                        continue
                    if is_discrete_column(filtered, c):
                        all_vals = (
                            filtered.get_column(c).drop_nulls().cast(str).unique().sort().to_list()
                        )
                        sel = st.multiselect(
                            f"Allowed values: {c}",
                            options=all_vals,
                            default=all_vals,
                            key=f"panel_filter_vals_{pid}_{c}",
                        )
                        if sel and len(sel) < len(all_vals):
                            constraints[c] = SetConstraint(column=c, values=list(sel))
                    else:
                        col_df = filtered.select(pl.col(c).cast(pl.Float64).drop_nulls())
                        if col_df.is_empty():
                            continue
                        lo = float(col_df.min().item())
                        hi = float(col_df.max().item())
                        if lo == hi:
                            continue
                        r = st.slider(
                            f"Range: {c}",
                            min_value=float(lo),
                            max_value=float(hi),
                            value=(float(lo), float(hi)),
                            key=f"panel_filter_rng_{pid}_{c}",
                        )
                        if r[0] > lo or r[1] < hi:
                            constraints[c] = RangeConstraint(column=c, min_value=float(r[0]), max_value=float(r[1]))

    if remove_ids:
        panels = [p for p in panels if int(p.get("id", -1)) not in set(remove_ids)]
        st.session_state["_viz_panels"] = panels
        st.rerun()
        return
    st.session_state["_viz_panels"] = panels
    mongo_filters = constraints_to_mongo(constraints)
    predicates = constraints_to_predicates(constraints)
    st.session_state["_viz_global_constraints"] = constraints
    st.session_state["_viz_global_constraints_mongo"] = mongo_filters
    st.session_state["_viz_global_constraints_predicates"] = predicates
    st.session_state["_viz_layout_doc"] = {
        "panels": panels,
        "filters": mongo_filters,
        "predicates": predicates,
    }
    st.caption(f"Active crossfilters: {', '.join(sorted(constraints.keys())) if constraints else 'none'}")
    with st.expander("Crossfilter JSON (Mongo syntax)", expanded=False):
        st.code(json.dumps(st.session_state["_viz_layout_doc"], indent=2, default=str), language="json")

    for panel in panels:
        pid = int(panel["id"])
        dims = int(panel.get("dims", 1))
        cols = list(panel.get("columns", []))
        ptype = str(panel.get("plot_type", "distribution"))
        scales = list(panel.get("scales", ["auto"] * dims))
        view_df = apply_constraints(filtered, constraints, exclude_columns=set(cols))
        sampled = view_df
        max_rows = 60_000
        if view_df.height > max_rows:
            sampled = view_df.sample(n=max_rows, seed=42)
        with st.expander(f"Rendered panel {pid}", expanded=True):
            if sampled.height < view_df.height:
                st.caption(
                    f"Plot sampled to {sampled.height:,} rows from {view_df.height:,}."
                )
            _render_panel_plot(
                sampled.to_pandas(),
                panel_id=pid,
                dims=dims,
                columns=cols,
                scale_options=scales,
                plot_type=ptype,
                time_col=used_time_col,
            )

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

    st.subheader("Feature-Target Association")
    numeric_cols = [c for c, dt in zip(filtered.columns, filtered.dtypes) if dt.is_numeric()]
    if len(numeric_cols) < 2:
        st.info("Need at least 2 numeric columns (target + feature) for association suggestions.")
    else:
        assoc_cols = st.columns([2, 2, 1, 1])
        with assoc_cols[0]:
            target_col = st.selectbox("Target", options=numeric_cols, index=0)
        with assoc_cols[1]:
            score_range = st.slider(
                "Mutual information score range",
                min_value=0.0,
                max_value=1.0,
                value=(0.6, 0.8),
                step=0.01,
            )
        with assoc_cols[2]:
            max_scatter = st.number_input("Max plots", min_value=1, max_value=20, value=5, step=1)
        with assoc_cols[3]:
            sample_cap = st.number_input(
                "Sample cap",
                min_value=1_000,
                max_value=200_000,
                value=20_000,
                step=1_000,
            )

        suggested, warn = suggest_feature_target_associations(
            filtered,
            target=target_col,
            min_score=float(score_range[0]),
            max_score=float(score_range[1]),
            max_suggestions=int(max_scatter),
            sample_rows=int(sample_cap),
        )
        if warn:
            st.info(warn)
        if suggested:
            table = pd.DataFrame(
                [{"feature": s.feature, "score": round(float(s.score), 4)} for s in suggested]
            )
            st.dataframe(table, use_container_width=True, hide_index=True)
            for row in suggested:
                pair = (
                    filtered.select([row.feature, target_col])
                    .drop_nulls()
                    .sample(n=min(int(sample_cap), filtered.height), seed=42)
                    .to_pandas()
                )
                st.plotly_chart(
                    px.scatter(
                        pair,
                        x=row.feature,
                        y=target_col,
                        title=f"{row.feature} vs {target_col} (MI={row.score:.3f})",
                    ),
                    use_container_width=True,
                )
        elif not warn:
            st.info("No features found in the selected mutual-information range.")

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

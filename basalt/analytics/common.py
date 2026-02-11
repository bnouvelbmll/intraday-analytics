from pydantic import BaseModel, Field
from typing import List, Dict, Any, Literal, Optional, Union
from abc import ABC

# --- Common Types ---
AggregationMethod = Literal[
    "First",
    "Last",
    "Min",
    "Max",
    "Mean",
    "Sum",
    "Std",
    "TWA",
    "VWA",
    "Median",
    "NotionalWeighted",
]

# Metric hint registry (module can register hints for schema metadata).
METRIC_HINTS: list[dict] = []
_METRIC_HINT_KEYS: set[tuple] = set()


def _register_metric_hint(data: dict):
    key = (
        data.get("module"),
        data.get("pattern"),
        data.get("default_agg"),
        data.get("weight_col"),
    )
    if key in _METRIC_HINT_KEYS:
        return
    _METRIC_HINT_KEYS.add(key)
    METRIC_HINTS.append(data)


def metric_hint(
    module: str,
    pattern: str,
    default_agg: AggregationMethod,
    weight_col: str | None = None,
):
    def _decorator(fn):
        _register_metric_hint(
            {
                "module": module,
                "pattern": pattern,
                "default_agg": default_agg,
                "weight_col": weight_col,
            }
        )
        return fn

    return _decorator


METRIC_DOCS: list[dict] = []
_METRIC_DOC_KEYS: set[tuple] = set()


def _register_metric_doc(data: dict):
    key = (
        data.get("module"),
        data.get("pattern"),
        data.get("template"),
        data.get("unit"),
        data.get("group"),
        data.get("group_role"),
        data.get("group_semantics"),
    )
    if key in _METRIC_DOC_KEYS:
        return
    _METRIC_DOC_KEYS.add(key)
    METRIC_DOCS.append(data)


def metric_doc(
    module: str,
    pattern: str,
    template: str,
    unit: str | None = None,
    group: str | None = None,
    group_role: str | None = None,
    group_semantics: str | None = None,
    description: str | None = None,
):
    def _decorator(fn):
        _register_metric_doc(
            {
                "module": module,
                "pattern": pattern,
                "template": template,
                "unit": unit,
                "group": group,
                "group_role": group_role,
                "group_semantics": group_semantics,
                "description": description,
            }
        )
        return fn

    return _decorator


Side = Literal["Bid", "Ask"]
MarketState = Literal[
    "AUCTION_ON_DEMAND",
    "CLOSED",
    "CLOSING_AUCTION",
    "CONDITIONAL",
    "CONTINUOUS_TRADING",
    "CONTINUOUS_TRADING_PRIMARY_CLOSED",
    "HALTED",
    "INTRADAY_AUCTION",
    "NOT_APPLICABLE",
    "OPENING_AUCTION",
    "POST_TRADE",
    "PRE_OPEN",
    "UNKNOWN",
    "UNSCHEDULED_AUCTION",
]


class CombinatorialMetricConfig(BaseModel, ABC):
    """
    Base class for metric configurations that support combinatorial expansion.

    Fields defined here can be lists or scalars; `expand()` produces the
    Cartesian product of all list-valued fields to build concrete metrics.
    """

    aggregations: List[AggregationMethod] = Field(
        default_factory=lambda: ["Last"],
        description="Aggregations to apply.",
        json_schema_extra={
            "long_doc": "List of aggregation operators applied to each generated series.\n"
            "Each operator produces an output column per metric variant.\n"
            "Examples: ['Last'] yields a single column; ['Sum','Mean'] yields two.\n"
            "If you include TWA/VWA, the module must provide the weight column.\n"
            "Aggregation names are consumed by module-specific expressions.\n"
            "Implementation: `CombinatorialMetricConfig.expand()` and\n"
            "`AnalyticSpec.expressions()` in `basalt/analytics_base.py`.\n"
            "L2 TW metrics use DT duration for time-weighted averages.\n"
            "Trade and L3 metrics treat Sum/Mean/Last in the usual group-by sense.\n"
            "Choose fewer aggregations to reduce output size and compute time.",
        },
    )

    output_name_pattern: Optional[str] = Field(
        None,
        description=(
            "Optional pattern for naming the output column. "
            "If empty, module-specific default naming is used. "
            "Available vars: {field_names}."
        ),
        json_schema_extra={
            "section": "Advanced",
            "long_doc": "If provided, overrides module naming for the generated metric columns.\n"
            "Variables in braces map to metric fields from the config.\n"
            "Example: '{measures}_{sides}_{aggregations}' or '{measure}{side}{agg}'.\n"
            "If omitted, each module uses its default naming scheme.\n"
            "The pattern is applied after combinatorial expansion.\n"
            "Used by `AnalyticSpec` when generating column aliases.\n"
            "Implementation is in `basalt/analytics_base.py`.\n"
            "Be consistent across passes if you join outputs downstream.\n"
            "Changing the pattern changes column names and downstream expectations.\n",
        },
    )

    market_states: Optional[Union[MarketState, List[MarketState]]] = Field(
        None,
        description="Filter by MarketState.",
        json_schema_extra={
            "long_doc": "If set, restricts metrics to the specified MarketState(s).\n"
            "Example: 'CONTINUOUS_TRADING' filters to continuous trading only.\n"
            "A list generates one metric variant per state via `expand()`.\n"
            "This increases output columns linearly with the number of states.\n"
            "Used in module-specific filters before aggregation.\n"
            "Applies to trades and L2/L3 metrics when MarketState is present.\n"
            "Implementation: filters in `Trade*Analytic` and related modules.\n"
            "If MarketState is missing in the input, filtering is skipped.\n"
            "Be careful mixing states across venues with different state taxonomies.\n"
        },
    )

    def expand(self) -> List[Dict[str, Any]]:
        """
        Generates the Cartesian product of all list-based fields.
        Returns a list of dictionaries, where each dictionary represents a single, concrete metric.
        """
        import itertools

        # 1. Separate single values from lists
        single_params = {}
        list_params = {}

        # We iterate over the model fields to find which ones are lists
        # Exclude base fields that are applied to *all* variants
        exclude_fields = {"aggregations", "output_name_pattern"}

        for field_name, value in self.model_dump().items():
            if field_name in exclude_fields:
                continue

            if isinstance(value, list) and not isinstance(value, str):
                list_params[field_name] = value
            else:
                single_params[field_name] = value

        # 2. Cartesian Product
        keys = list(list_params.keys())
        values = list(list_params.values())

        expanded = []
        if not keys:
            expanded.append(single_params)
        else:
            for combination in itertools.product(*values):
                item = single_params.copy()
                for i, key in enumerate(keys):
                    item[key] = combination[i]
                expanded.append(item)

        return expanded


def apply_aggregation(expr, agg: AggregationMethod):
    if agg == "First":
        return expr.first()
    if agg == "Last":
        return expr.last()
    if agg == "Min":
        return expr.min()
    if agg == "Max":
        return expr.max()
    if agg == "Mean":
        return expr.mean()
    if agg == "Sum":
        return expr.sum()
    if agg == "Std":
        return expr.std()
    if agg == "Median":
        return expr.median()
    return None

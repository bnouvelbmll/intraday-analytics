from pydantic import BaseModel, Field
from typing import List, Dict, Any, Literal, Optional, Union
from abc import ABC

# --- Common Types ---
AggregationMethod = Literal[
    "First", "Last", "Min", "Max", "Mean", "Sum", "Std", "TWA", "VWA", "Median"
]
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
    """

    aggregations: List[AggregationMethod] = Field(
        default_factory=lambda: ["Last"],
        description="List of aggregations to apply (e.g., 'TWA', 'Max').",
    )

    output_name_pattern: Optional[str] = Field(
        None,
        description="Optional pattern for naming the output column. Available vars: {field_names}.",
    )

    market_states: Optional[Union[MarketState, List[MarketState]]] = Field(
        None,
        description="Filter by MarketState. If list, generates variants for each state.",
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

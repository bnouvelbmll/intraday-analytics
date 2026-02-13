from __future__ import annotations

import polars as pl

from basalt.optimize.presets import (
    autogluon_fast_model_factory,
    dataset_builder_from_last_pass_output,
    history_guided_generator,
    simple_linear_model_factory,
)


def test_presets_dataset_builder_and_model(tmp_path):
    out_path = tmp_path / "out_sample2d_pass1_2026-01-01_2026-01-01.parquet"
    df = pl.DataFrame(
        {
            "ListingId": [1] * 20,
            "TimeBucket": list(range(20)),
            "FeatureA": [float(v) for v in range(20)],
            "FutureReturn": [float(v) * 0.1 for v in range(20)],
        }
    )
    df.write_parquet(out_path)

    cfg = {
        "START_DATE": "2026-01-01",
        "END_DATE": "2026-01-01",
        "DATASETNAME": "sample2d",
        "OUTPUT_TARGET": {"path_template": str(tmp_path / "out_{datasetname}_{start_date}_{end_date}.parquet")},
        "PASSES": [{"name": "pass1"}],
    }
    split = dataset_builder_from_last_pass_output(config=cfg)
    assert split.metadata["target_column"] == "FutureReturn"
    assert split.metadata["feature_count"] == 1
    model = simple_linear_model_factory()
    model.fit(split.X_train, split.y_train)
    preds = model.predict(split.X_eval)
    assert len(preds) == len(split.y_eval)


def test_history_guided_generator_shapes():
    import random

    search_space = {
        "params": {
            "PASSES[0].time_bucket_seconds": {"type": "choice", "values": [1, 5, 10]},
            "PASSES[0].l2_analytics.levels": {"type": "int", "low": 1, "high": 10},
        }
    }
    rng = random.Random(1)
    first = history_guided_generator(
        trial_id=1,
        search_space=search_space,
        rng=rng,
        history=[],
        base_config={},
    )
    assert "PASSES[0].time_bucket_seconds" in first
    assert "PASSES[0].l2_analytics.levels" in first


def test_autogluon_fast_model_factory_returns_model():
    model = autogluon_fast_model_factory()
    assert model.__class__.__name__ == "AutoGluonTabularModel"
    assert model.fit_params.get("presets") == "medium_quality_faster_train"

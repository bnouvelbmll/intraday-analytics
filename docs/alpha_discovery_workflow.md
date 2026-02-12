# Alpha Discovery Workflow

This guide shows how to use existing Basalt modules plus `optimize`, `objective_functions`, and `models` to search for feature configurations ("alphas") against a predictive objective, without custom user packages.

## 1. Install required packages

```bash
pip install -e .
pip install -e '.[optimize,objective_functions,models]'
```

Optional trackers:

```bash
pip install mlflow wandb
```

Optional AutoGluon model backend:

```bash
pip install autogluon
```

## 2. Start from an existing pipeline

Use one of your current pipelines (for example `demo/02_multi_pass.py`) and confirm it runs:

```bash
basalt pipeline run --pipeline demo/02_multi_pass.py --date 2026-02-01
```

## 3. Ensure output path is local parquet for direct optimization

In your pipeline YAML (or config UI), set:

```yaml
USER_CONFIG:
  OUTPUT_TARGET:
    type: parquet
    path_template: "/tmp/basalt_optimize_{datasetname}_{start_date}_{end_date}.parquet"
```

The built-in dataset builder reads the latest pass output parquet from this rendered path.

## 4. Define a search space

Create `search_space.yaml`:

```yaml
params:
  PASSES[0].l2_analytics.levels:
    type: choice
    values: [1, 3, 5, 10]
  PASSES[0].time_bucket_seconds:
    type: choice
    values: [1, 5, 10, 30]
  PASSES[0].reaggregate_analytics.enabled:
    type: bool
```

Each key is an override path in `USER_CONFIG`.

## 5. Choose scoring mode

Use model + objective mode:

- `--model_factory basalt.optimize.presets:simple_linear_model_factory`
- `--dataset_builder basalt.optimize.presets:dataset_builder_from_last_pass_output`
- `--objectives` as either:
  - built-in names such as `mae,directional_accuracy,rmse`, or
  - callable path `module:function`

You can also use custom `--score_fn module:function`, but that path is optional.

## 6. Run optimization locally

### Model/objective mode

```bash
basalt optimize run \
  --pipeline demo/02_multi_pass.py \
  --search_space_file search_space.yaml \
  --trials 20 \
  --executor direct \
  --model_factory basalt.optimize.presets:simple_linear_model_factory \
  --dataset_builder basalt.optimize.presets:dataset_builder_from_last_pass_output \
  --objectives mae,directional_accuracy \
  --objective directional_accuracy
```

Outputs are written to `optimize_results/`:

- `trials.jsonl`: all trial results
- `summary.json`: best trial and run stats

### AutoGluon variant (same workflow)

```bash
basalt optimize run \
  --pipeline demo/02_multi_pass.py \
  --search_space_file search_space.yaml \
  --trials 20 \
  --executor direct \
  --model_factory basalt.optimize.presets:autogluon_fast_model_factory \
  --dataset_builder basalt.optimize.presets:dataset_builder_from_last_pass_output \
  --objectives mae,directional_accuracy \
  --objective directional_accuracy
```

## 7. Use advanced search generators (Optuna-style)

You can plug a custom generator:

```bash
basalt optimize run \
  --pipeline demo/02_multi_pass.py \
  --search_space_file search_space.yaml \
  --trials 20 \
  --executor direct \
  --model_factory basalt.optimize.presets:simple_linear_model_factory \
  --dataset_builder basalt.optimize.presets:dataset_builder_from_last_pass_output \
  --objectives mae \
  --search_generator basalt.optimize.presets:history_guided_generator
```

The generator receives `trial_id`, `search_space`, `rng`, `history`, `base_config` and must return `dict[path, value]`.

## 8. Track runs in MLflow or Weights & Biases

MLflow:

```bash
basalt optimize run \
  --pipeline demo/02_multi_pass.py \
  --search_space_file search_space.yaml \
  --trials 20 \
  --executor direct \
  --model_factory basalt.optimize.presets:simple_linear_model_factory \
  --dataset_builder basalt.optimize.presets:dataset_builder_from_last_pass_output \
  --objectives mae,directional_accuracy \
  --tracker mlflow \
  --tracker_experiment basalt-alpha-search \
  --tracker_uri http://localhost:5000
```

W&B:

```bash
basalt optimize run \
  --pipeline demo/02_multi_pass.py \
  --search_space_file search_space.yaml \
  --trials 20 \
  --executor direct \
  --model_factory basalt.optimize.presets:simple_linear_model_factory \
  --dataset_builder basalt.optimize.presets:dataset_builder_from_last_pass_output \
  --objectives mae,directional_accuracy \
  --tracker wandb \
  --tracker_project basalt-alpha-search \
  --tracker_mode offline
```

## 9. Dispatch distributed trials

For remote execution:

```bash
basalt optimize run \
  --pipeline demo/02_multi_pass.py \
  --search_space_file search_space.yaml \
  --trials 20 \
  --executor bmll \
  --instance_size 64
```

`bmll` and `ec2` modes submit jobs and log submission metadata; direct in-process scoring is only for `executor=direct`.

## 10. Run optimization via MCP

`optimize_run` and `optimize_summary` are available from the MCP package and server.

Example tool payload:

```json
{
  "pipeline": "demo/02_multi_pass.py",
  "search_space_file": "search_space.yaml",
  "trials": 20,
  "executor": "direct",
  "model_factory": "basalt.optimize.presets:simple_linear_model_factory",
  "dataset_builder": "basalt.optimize.presets:dataset_builder_from_last_pass_output",
  "objectives": "mae,directional_accuracy",
  "objective": "directional_accuracy"
}
```

## 11. Recommended baseline objective setups

Typical finance targets:

- Price-at-horizon regression:
  - objective: `mae` or `rmse`
- Next-move direction:
  - objective: `directional_accuracy`
- Volatility/volume shift prediction:
  - objective: `mae` or `mse`

Start with `--trials 20`, validate signal stability, then scale trials and expand search space.

## 12. Required output columns for built-in presets

`dataset_builder_from_last_pass_output` expects:

- At least one target-like column in the last pass output:
  - `FutureReturn`, `FutureMidReturn`, `NextMidReturn`, `Return`, `Label`, or `Target`
- At least one numeric feature column (excluding keys like `ListingId`, `TimeBucket`, `Timestamp`).

from __future__ import annotations

from . import service


def create_server():
    from fastmcp import FastMCP

    mcp = FastMCP("basalt-runs")

    @mcp.tool()
    def capabilities() -> dict:
        """List available executors and MCP features."""
        return service.list_capabilities()

    @mcp.tool()
    def list_plugins() -> dict:
        """List discovered Basalt plugins."""
        return service.list_plugins()

    @mcp.tool()
    def list_metrics(
        module: str | None = None,
        limit: int | None = None,
    ) -> dict:
        """List analytics metrics metadata from registered analytic docs."""
        return service.list_metrics(module=module, limit=limit)

    @mcp.tool()
    def inspect_metric_source(
        metric: str,
        module: str | None = None,
        context_lines: int = 20,
    ) -> dict:
        """Inspect source-code snippets that define or reference a metric."""
        return service.inspect_metric_source(
            metric=metric,
            module=module,
            context_lines=context_lines,
        )

    @mcp.tool()
    def configure_job(
        pipeline: str,
        executor: str = "bmll",
        instance_size: int | None = None,
        conda_env: str | None = None,
        max_runtime_hours: int | None = None,
        cron: str | None = None,
        cron_timezone: str | None = None,
        schedule_name: str | None = None,
    ) -> dict:
        """Configure a pipeline for job execution defaults/schedules."""
        return service.configure_job(
            pipeline=pipeline,
            executor=executor,
            instance_size=instance_size,
            conda_env=conda_env,
            max_runtime_hours=max_runtime_hours,
            cron=cron,
            cron_timezone=cron_timezone,
            schedule_name=schedule_name,
        )

    @mcp.tool()
    def run_job(
        pipeline: str,
        executor: str = "bmll",
        name: str | None = None,
        instance_size: int | None = None,
        conda_env: str | None = None,
        cron: str | None = None,
        cron_timezone: str | None = None,
        overwrite_existing: bool = False,
        job: str | None = None,
        partition: str | None = None,
        run_id: str | None = None,
        instance_ref_file: str | None = None,
    ) -> dict:
        """Run a pipeline using the chosen executor."""
        return service.run_job(
            pipeline=pipeline,
            executor=executor,
            name=name,
            instance_size=instance_size,
            conda_env=conda_env,
            cron=cron,
            cron_timezone=cron_timezone,
            overwrite_existing=overwrite_existing,
            job=job,
            partition=partition,
            run_id=run_id,
            instance_ref_file=instance_ref_file,
        )

    @mcp.tool()
    def recent_runs(
        executor: str = "bmll",
        limit: int = 20,
        status: str | None = None,
        dagster_home: str | None = None,
    ) -> dict:
        """List recent runs for an executor."""
        return service.recent_runs(
            executor=executor,
            limit=limit,
            status=status,
            dagster_home=dagster_home,
        )

    @mcp.tool()
    def success_rate(
        executor: str = "bmll",
        limit: int = 100,
        status: str | None = None,
        dagster_home: str | None = None,
    ) -> dict:
        """Compute recent run success rate for an executor."""
        return service.success_rate(
            executor=executor,
            limit=limit,
            status=status,
            dagster_home=dagster_home,
        )

    @mcp.tool()
    def materialized_partitions(
        asset_key: str,
        limit: int = 200,
        dagster_home: str | None = None,
    ) -> dict:
        """List recent Dagster materialized partitions for an asset key."""
        return service.materialized_partitions(
            asset_key=asset_key,
            limit=limit,
            dagster_home=dagster_home,
        )

    @mcp.tool()
    def optimize_run(
        pipeline: str,
        search_space: dict | None = None,
        search_space_file: str | None = None,
        trials: int = 20,
        executor: str = "direct",
        score_fn: str | None = None,
        maximize: bool = True,
        seed: int = 0,
        output_dir: str = "optimize_results",
        tracker: str = "none",
        tracker_project: str | None = None,
        tracker_experiment: str | None = None,
        tracker_run_name: str | None = None,
        tracker_tags: dict | str | None = None,
        tracker_uri: str | None = None,
        tracker_mode: str | None = None,
        model_factory: str | None = None,
        dataset_builder: str | None = None,
        objectives: str | None = None,
        objective: str | None = None,
        use_aggregate: bool = False,
        search_generator: str | None = None,
        instance_size: int | None = None,
        delete_after: bool | None = None,
    ) -> dict:
        """Run optimize pipeline with optional model/objective and tracker integration."""
        return service.optimize_run(
            pipeline=pipeline,
            search_space=search_space,
            search_space_file=search_space_file,
            trials=trials,
            executor=executor,
            score_fn=score_fn,
            maximize=maximize,
            seed=seed,
            output_dir=output_dir,
            tracker=tracker,
            tracker_project=tracker_project,
            tracker_experiment=tracker_experiment,
            tracker_run_name=tracker_run_name,
            tracker_tags=tracker_tags,
            tracker_uri=tracker_uri,
            tracker_mode=tracker_mode,
            model_factory=model_factory,
            dataset_builder=dataset_builder,
            objectives=objectives,
            objective=objective,
            use_aggregate=use_aggregate,
            search_generator=search_generator,
            instance_size=instance_size,
            delete_after=delete_after,
        )

    @mcp.tool()
    def optimize_summary(output_dir: str = "optimize_results") -> dict:
        """Read optimize summary and trials metadata from output directory."""
        return service.optimize_summary(output_dir=output_dir)

    return mcp


def run_server(
    *,
    transport: str = "stdio",
    host: str = "127.0.0.1",
    port: int = 8000,
):
    mcp = create_server()
    try:
        return mcp.run(transport=transport, host=host, port=int(port))
    except TypeError:
        return mcp.run()

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
    def configure_job(
        pipeline: str,
        executor: str = "ec2",
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
        executor: str = "ec2",
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
        executor: str = "ec2",
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
        executor: str = "ec2",
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

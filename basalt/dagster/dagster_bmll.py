from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional

from dagster import DagsterRun
from dagster._core.instance.ref import InstanceRef
from dagster._core.launcher import RunLauncher
from dagster._core.storage.pipeline_run import DagsterRunStatus
from dagster._serdes import serialize_value

from basalt.executors.bmll import submit_instance_job
from basalt.configuration import BMLLJobConfig


class BMLLRunLauncher(RunLauncher):
    def __init__(self, job_config: Optional[BMLLJobConfig] = None):
        super().__init__()
        self._job_config = job_config or BMLLJobConfig()

    def launch_run(self, context, run: DagsterRun) -> None:
        if run.status != DagsterRunStatus.NOT_STARTED:
            return

        instance_ref = context.instance.get_ref()
        jobs_dir = Path(self._job_config.jobs_dir)
        jobs_dir.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        instance_ref_path = jobs_dir / f"instance_ref_{stamp}.json"
        instance_ref_path.write_text(
            json.dumps(serialize_value(instance_ref)), encoding="utf-8"
        )

        script_path = jobs_dir / f"dagster_run_{run.run_id}.sh"
        script_path.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env bash",
                    "set -euo pipefail",
                    "python -m basalt.basalt dagster run "
                    f"--run_id {run.run_id} "
                    f"--instance_ref_file {instance_ref_path}",
                ]
            ),
            encoding="utf-8",
        )
        script_path.chmod(0o755)

        tag_instance_size = run.tags.get("bmll/instance_size")
        tag_conda_env = run.tags.get("bmll/conda_env")
        tag_max_runtime = run.tags.get("bmll/max_runtime_hours")
        tag_max_concurrency = run.tags.get("bmll/max_concurrent_instances")
        tag_log_path = run.tags.get("bmll/log_path")

        instance_size = (
            int(tag_instance_size)
            if tag_instance_size is not None
            else self._job_config.default_instance_size
        )
        conda_env = tag_conda_env or self._job_config.default_conda_env

        config = self._job_config.model_copy()
        if tag_max_runtime is not None:
            config = config.model_copy(update={"max_runtime_hours": int(tag_max_runtime)})
        if tag_max_concurrency is not None:
            config = config.model_copy(
                update={"max_concurrent_instances": int(tag_max_concurrency)}
            )
        if tag_log_path:
            config = config.model_copy(update={"log_path": tag_log_path})

        submit_instance_job(
            str(script_path),
            name=f"dagster_{run.run_id}",
            instance_size=instance_size,
            conda_env=conda_env,
            delete_after=self._job_config.delete_job_after,
            config=config,
        )

        context.instance.report_run_launched(run)

    def terminate(self, run_id: str) -> bool:
        return False

    def can_terminate(self, run_id: str) -> bool:
        return False

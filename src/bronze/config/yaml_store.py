"""
yaml_store.py
-------------
V1 implementation of ConfigStore — reads job configs from a YAML file.

To swap to Delta later, implement DeltaConfigStore(ConfigStore) and change
the one line in scripts/run.py that instantiates YamlConfigStore.
Nothing else in the codebase changes.
"""

from __future__ import annotations
import yaml
from pathlib import Path
from pydantic import ValidationError

from bronze.config.config_store import ConfigStore
from bronze.config.models import JobConfig, PipelineConfig


class YamlConfigStore(ConfigStore):
    """
    Loads all job configs from a single YAML file at initialisation time.
    Validates the entire file via Pydantic before any job runs.
    """

    def __init__(self, config_path: str | Path) -> None:
        self._path = Path(config_path)
        self._jobs: dict[str, JobConfig] = {}
        self._load()

    # ------------------------------------------------------------------
    # Public interface (implements ConfigStore)
    # ------------------------------------------------------------------

    def get_job(self, job_id: str) -> JobConfig:
        """Return a JobConfig by job_id. Raises KeyError if not found."""
        if job_id not in self._jobs:
            available = list(self._jobs.keys())
            raise KeyError(
                f"job_id '{job_id}' not found in {self._path}. "
                f"Available jobs: {available}"
            )
        return self._jobs[job_id]

    def list_jobs(self) -> list[JobConfig]:
        """Return all JobConfig objects defined in the YAML file."""
        return list(self._jobs.values())

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load(self) -> None:
        if not self._path.exists():
            raise FileNotFoundError(
                f"Config file not found: {self._path.resolve()}"
            )

        with self._path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        if raw is None:
            raise ValueError(f"Config file is empty: {self._path}")

        try:
            pipeline = PipelineConfig.model_validate(raw)
        except ValidationError as exc:
            raise ValueError(
                f"Config validation failed in {self._path}:\n{exc}"
            ) from exc

        # Check for duplicate job_ids
        seen: set[str] = set()
        for job in pipeline.jobs:
            if job.job_id in seen:
                raise ValueError(
                    f"Duplicate job_id '{job.job_id}' found in {self._path}"
                )
            seen.add(job.job_id)

        self._jobs = {job.job_id: job for job in pipeline.jobs}

    def __repr__(self) -> str:
        return (
            f"YamlConfigStore(path={self._path}, "
            f"jobs={list(self._jobs.keys())})"
        )
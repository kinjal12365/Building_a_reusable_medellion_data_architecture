"""
config_store.py
---------------
Abstract base class for the config store.
Swap YamlConfigStore for DeltaConfigStore later with zero changes elsewhere.
"""

from abc import ABC, abstractmethod
from bronze.config.models import JobConfig


class ConfigStore(ABC):

    @abstractmethod
    def get_job(self, job_id: str) -> JobConfig:
        """Return a single JobConfig by job_id. Raise KeyError if not found."""
        ...

    @abstractmethod
    def list_jobs(self) -> list[JobConfig]:
        """Return all JobConfig objects defined in the store."""
        ...
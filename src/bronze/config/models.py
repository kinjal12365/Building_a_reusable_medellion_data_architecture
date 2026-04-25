"""
models.py
---------
Pydantic v2 models that validate and represent a job config entry.
These map 1:1 to the YAML structure (and later to Delta config table rows).
"""

from __future__ import annotations
from typing import Literal
from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Supported file formats
# ---------------------------------------------------------------------------

FileFormat = Literal["csv", "json", "parquet"]


# ---------------------------------------------------------------------------
# Format-specific reader options
# ---------------------------------------------------------------------------

class CsvOptions(BaseModel):
    header: bool = True
    delimiter: str = ","
    encoding: str = "utf-8"
    multiline: bool = False


class JsonOptions(BaseModel):
    multiline: bool = False


class ParquetOptions(BaseModel):
    pass  # Parquet has no permissive-read options needed for Bronze v1


# ---------------------------------------------------------------------------
# Source block
# ---------------------------------------------------------------------------

class SourceConfig(BaseModel):
    uri: str = Field(
        ...,
        description="Cloud storage URI — abfss://, s3a://, gs://, or local path."
    )
    format: FileFormat | None = Field(
        default=None,
        description="Force a specific format. None means auto-detect."
    )
    pattern: str = Field(
        default="*",
        description="Glob pattern to filter files inside the URI path."
    )

    @field_validator("uri")
    @classmethod
    def uri_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("source.uri must not be empty")
        return v


# ---------------------------------------------------------------------------
# Bronze destination block
# ---------------------------------------------------------------------------

class BronzeConfig(BaseModel):
    table_name: str = Field(
        ...,
        description="Name of the Bronze table, e.g. bronze_customers."
    )
    location: str = Field(
        ...,
        description="Cloud storage path where the Parquet table is written."
    )

    @field_validator("table_name")
    @classmethod
    def table_name_must_be_prefixed(cls, v: str) -> str:
        if not v.startswith("bronze_"):
            raise ValueError(f"table_name must start with 'bronze_', got: '{v}'")
        return v


# ---------------------------------------------------------------------------
# Per-format options block (all optional)
# ---------------------------------------------------------------------------

class FormatOptions(BaseModel):
    csv: CsvOptions = Field(default_factory=CsvOptions)
    json_opts: JsonOptions = Field(default_factory=JsonOptions, alias="json")
    parquet: ParquetOptions = Field(default_factory=ParquetOptions)

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# Top-level job config
# ---------------------------------------------------------------------------

class JobConfig(BaseModel):
    job_id: str = Field(..., description="Unique identifier for this ingestion job.")
    dataset: str = Field(..., description="Logical dataset name, e.g. customers.")
    source: SourceConfig
    bronze: BronzeConfig
    options: FormatOptions = Field(default_factory=FormatOptions)

    @model_validator(mode="after")
    def dataset_must_match_table(self) -> JobConfig:
        expected_suffix = self.dataset.lower()
        if not self.bronze.table_name.endswith(expected_suffix):
            raise ValueError(
                f"bronze.table_name '{self.bronze.table_name}' "
                f"should end with dataset name '{expected_suffix}'"
            )
        return self


# ---------------------------------------------------------------------------
# Root wrapper (matches top-level 'jobs:' key in YAML)
# ---------------------------------------------------------------------------

class PipelineConfig(BaseModel):
    jobs: list[JobConfig] = Field(..., min_length=1)
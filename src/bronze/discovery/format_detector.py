"""
format_detector.py
------------------
Pure function — no Spark, no I/O beyond magic-byte sniffing.

Priority order:
  1. Config override (format explicitly set in YAML)
  2. File extension
  3. Magic bytes (first 4 bytes of the file)

Returns one of: "csv", "json", "parquet"
Raises ValueError if format cannot be determined.
"""

from __future__ import annotations
from pathlib import PurePosixPath


# ---------------------------------------------------------------------------
# Known extensions
# ---------------------------------------------------------------------------

EXTENSION_MAP: dict[str, str] = {
    ".csv"     : "csv",
    ".tsv"     : "csv",   # treat TSV as CSV (delimiter handled in options)
    ".json"    : "json",
    ".jsonl"   : "json",
    ".ndjson"  : "json",
    ".parquet" : "parquet",
    ".pq"      : "parquet",
}


# ---------------------------------------------------------------------------
# Magic bytes
# ---------------------------------------------------------------------------

# Parquet files always start with PAR1
PARQUET_MAGIC = b"PAR1"

# JSON files start with { or [ (after optional whitespace)
JSON_START_CHARS = {ord("{"), ord("[")}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_format(
    file_path: str,
    config_override: str | None = None,
    read_bytes_fn: "callable[[str, int], bytes] | None" = None,
) -> str:
    """
    Detect the format of a file.

    Args:
        file_path:       Full URI or local path to the file.
        config_override: If set in YAML (e.g. "csv"), returns immediately.
        read_bytes_fn:   Optional callable(path, n) -> bytes for magic sniffing.
                         If None, magic-byte sniffing is skipped (safe for
                         cloud paths where local open() won't work).

    Returns:
        One of: "csv", "json", "parquet"

    Raises:
        ValueError: If format cannot be determined.
    """

    # ------------------------------------------------------------------
    # Step 1 — config override wins immediately
    # ------------------------------------------------------------------
    if config_override is not None:
        fmt = config_override.lower().strip()
        if fmt not in ("csv", "json", "parquet"):
            raise ValueError(
                f"Unsupported format override '{fmt}' for file: {file_path}. "
                f"Supported: csv, json, parquet"
            )
        return fmt

    # ------------------------------------------------------------------
    # Step 2 — extension
    # ------------------------------------------------------------------
    suffix = PurePosixPath(file_path).suffix.lower()
    if suffix in EXTENSION_MAP:
        return EXTENSION_MAP[suffix]

    # ------------------------------------------------------------------
    # Step 3 — magic bytes (only if a reader function is provided)
    # ------------------------------------------------------------------
    if read_bytes_fn is not None:
        try:
            header = read_bytes_fn(file_path, 4)
            return _sniff_magic(file_path, header)
        except Exception as exc:
            raise ValueError(
                f"Could not read magic bytes from '{file_path}': {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Cannot determine format
    # ------------------------------------------------------------------
    raise ValueError(
        f"Cannot detect format for '{file_path}'. "
        f"No extension match and no magic-byte reader provided. "
        f"Set 'format' explicitly in the YAML config."
    )


def _sniff_magic(file_path: str, header: bytes) -> str:
    """Sniff format from the first 4 bytes."""

    if len(header) >= 4 and header[:4] == PARQUET_MAGIC:
        return "parquet"

    # Strip whitespace and check first meaningful byte
    stripped = header.lstrip()
    if stripped and stripped[0] in JSON_START_CHARS:
        return "json"

    # Default fallback — if not Parquet or JSON, treat as CSV
    # (CSV has no magic bytes)
    return "csv"
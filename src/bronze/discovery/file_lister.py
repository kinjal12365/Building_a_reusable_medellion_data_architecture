"""
file_lister.py
--------------
Lists files under a URI, computes SHA-256 content hash per file,
and returns a list of DiscoveredFile dataclasses.

Supported URI schemes:
  abfss://   → Azure Data Lake Storage Gen2
  s3a://     → Amazon S3
  gs://      → Google Cloud Storage
  file://    → Local filesystem
  <no scheme> → Also treated as local path

The actual file bytes are read via the scheme-specific client.
Spark is NOT used here — discovery is lightweight and runs on the driver.
"""

from __future__ import annotations
import hashlib
import fnmatch
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# DiscoveredFile — what we return per file
# ---------------------------------------------------------------------------

@dataclass
class DiscoveredFile:
    uri: str                          # Full URI to the file
    scheme: str                       # abfss / s3a / gs / file
    file_name: str                    # Just the filename e.g. customers.csv
    content_hash: str                 # SHA-256 hex digest
    size_bytes: int                   # File size in bytes
    detected_format: str | None = None  # Filled in later by format_detector


# ---------------------------------------------------------------------------
# URI scheme detection
# ---------------------------------------------------------------------------

def _parse_scheme(uri: str) -> str:
    """Return the URI scheme: abfss, s3a, gs, or file."""
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()
    if scheme in ("abfss", "s3a", "gs"):
        return scheme
    # No scheme or 'file' → treat as local
    return "file"


# ---------------------------------------------------------------------------
# Hash helper
# ---------------------------------------------------------------------------

def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Per-scheme file listers
# ---------------------------------------------------------------------------

def _list_local(uri: str, pattern: str) -> list[tuple[str, bytes, int]]:
    """
    List files on the local filesystem.
    Returns list of (uri, content_bytes, size_bytes).
    """
    # Strip file:// prefix if present
    path_str = uri.replace("file://", "")
    base = Path(path_str).resolve()  # <-- resolve to absolute path

    if not base.exists():
        raise FileNotFoundError(f"Local path does not exist: {base}")

    results = []
    files = base.rglob("*") if "**" in pattern else base.glob(pattern)
    for f in files:
        if f.is_file():
            data = f.read_bytes()
            results.append((f.resolve().as_uri(), data, len(data)))
    return results


def _list_adls(uri: str, pattern: str) -> list[tuple[str, bytes, int]]:
    """
    List files in Azure Data Lake Storage Gen2.
    Requires: pip install azure-storage-file-datalake azure-identity
    """
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
        from azure.identity import DefaultAzureCredential
    except ImportError:
        raise ImportError(
            "ADLS support requires: pip install azure-storage-file-datalake azure-identity"
        )

    parsed = urlparse(uri)
    # abfss://container@account.dfs.core.windows.net/path
    container = parsed.username          # part before @
    account_host = parsed.hostname       # account.dfs.core.windows.net
    account_name = account_host.split(".")[0]
    prefix = parsed.path.lstrip("/")

    account_url = f"https://{account_host}"
    credential = DefaultAzureCredential()
    service = DataLakeServiceClient(account_url=account_url, credential=credential)
    fs_client = service.get_file_system_client(container)

    results = []
    paths = fs_client.get_paths(path=prefix or "/", recursive=True)
    for p in paths:
        if not p.is_directory:
            fname = p.name.split("/")[-1]
            if fnmatch.fnmatch(fname, pattern):
                file_client = fs_client.get_file_client(p.name)
                download = file_client.download_file()
                data = download.readall()
                full_uri = f"abfss://{container}@{account_host}/{p.name}"
                results.append((full_uri, data, len(data)))
    return results


def _list_s3(uri: str, pattern: str) -> list[tuple[str, bytes, int]]:
    """
    List files in Amazon S3.
    Requires: pip install boto3
    """
    try:
        import boto3
    except ImportError:
        raise ImportError("S3 support requires: pip install boto3")

    parsed = urlparse(uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    results = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            fname = key.split("/")[-1]
            if fnmatch.fnmatch(fname, pattern):
                response = s3.get_object(Bucket=bucket, Key=key)
                data = response["Body"].read()
                full_uri = f"s3a://{bucket}/{key}"
                results.append((full_uri, data, len(data)))
    return results


def _list_gcs(uri: str, pattern: str) -> list[tuple[str, bytes, int]]:
    """
    List files in Google Cloud Storage.
    Requires: pip install google-cloud-storage
    """
    try:
        from google.cloud import storage as gcs_storage
    except ImportError:
        raise ImportError(
            "GCS support requires: pip install google-cloud-storage"
        )

    parsed = urlparse(uri)
    bucket_name = parsed.netloc
    prefix = parsed.path.lstrip("/")

    client = gcs_storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = client.list_blobs(bucket_name, prefix=prefix)

    results = []
    for blob in blobs:
        fname = blob.name.split("/")[-1]
        if fnmatch.fnmatch(fname, pattern):
            data = blob.download_as_bytes()
            full_uri = f"gs://{bucket_name}/{blob.name}"
            results.append((full_uri, data, len(data)))
    return results


# ---------------------------------------------------------------------------
# Scheme router
# ---------------------------------------------------------------------------

_SCHEME_HANDLERS = {
    "abfss": _list_adls,
    "s3a"  : _list_s3,
    "gs"   : _list_gcs,
    "file" : _list_local,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def list_files(uri: str, pattern: str = "*") -> list[DiscoveredFile]:
    """
    List all files under a URI that match the glob pattern.
    Computes SHA-256 content hash for each file.

    Args:
        uri:     Cloud or local URI from the YAML config.
        pattern: Glob pattern e.g. "*.csv", "*", "data_*.json"

    Returns:
        List of DiscoveredFile dataclasses, one per matching file.

    Raises:
        ValueError:  If the URI scheme is not supported.
        ImportError: If the required cloud SDK is not installed.
    """
    scheme = _parse_scheme(uri)

    if scheme not in _SCHEME_HANDLERS:
        raise ValueError(
            f"Unsupported URI scheme '{scheme}' in: {uri}. "
            f"Supported: abfss, s3a, gs, file"
        )

    handler = _SCHEME_HANDLERS[scheme]
    raw_files = handler(uri, pattern)

    discovered = []
    for full_uri, data, size in raw_files:
        fname = full_uri.rstrip("/").split("/")[-1]
        discovered.append(
            DiscoveredFile(
                uri=full_uri,
                scheme=scheme,
                file_name=fname,
                content_hash=_sha256(data),
                size_bytes=size,
            )
        )

    return discovered
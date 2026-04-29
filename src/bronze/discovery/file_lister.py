from __future__ import annotations
import hashlib
import fnmatch
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


@dataclass
class DiscoveredFile:
    uri: str
    scheme: str
    file_name: str
    content_hash: str
    size_bytes: int
    detected_format: str | None = None


def _parse_scheme(uri: str) -> str:
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()
    if scheme in ("abfss", "s3a", "gs"):
        return scheme
    return "file"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _list_local(uri: str, pattern: str) -> list[tuple[str, bytes, int]]:
    path_str = uri.replace("file://", "")
    base = Path(path_str).resolve()
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
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.storage.filedatalake import StorageStreamDownloader

    parsed = urlparse(uri)
    container = parsed.username
    account_host = parsed.hostname
    account_name = account_host.split(".")[0]
    prefix = parsed.path.lstrip("/")

    import os
    account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

    account_url = f"https://{account_host}"

    if account_key:
        service = DataLakeServiceClient(
            account_url=account_url,
            credential=account_key
        )
    else:
        from azure.identity import DefaultAzureCredential
        service = DataLakeServiceClient(
            account_url=account_url,
            credential=DefaultAzureCredential()
        )

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
    import boto3
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
    from google.cloud import storage as gcs_storage
    parsed = urlparse(uri)
    bucket_name = parsed.netloc
    prefix = parsed.path.lstrip("/")
    client = gcs_storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    results = []
    for blob in blobs:
        fname = blob.name.split("/")[-1]
        if fnmatch.fnmatch(fname, pattern):
            data = blob.download_as_bytes()
            full_uri = f"gs://{bucket_name}/{blob.name}"
            results.append((full_uri, data, len(data)))
    return results


_SCHEME_HANDLERS = {
    "abfss": _list_adls,
    "s3a"  : _list_s3,
    "gs"   : _list_gcs,
    "file" : _list_local,
}


def list_files(uri: str, pattern: str = "*") -> list[DiscoveredFile]:
    scheme = _parse_scheme(uri)
    if scheme not in _SCHEME_HANDLERS:
        raise ValueError(f"Unsupported URI scheme '{scheme}' in: {uri}")
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
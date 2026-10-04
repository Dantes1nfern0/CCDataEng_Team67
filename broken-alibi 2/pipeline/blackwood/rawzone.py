"""The raw zone: original bytes, written once, never edited.

Locally this is a folder. Files are created with exclusive-create ("x" mode), so
an existing file can never be replaced, and each file gets a manifest holding
its SHA-256 and one SHA-256 per record, taken before any cleaning.
In production the same layout sits in S3 with Object Lock (compliance mode),
which stops anyone, admins included, from editing or deleting it."""
import hashlib
import json
import os
from pathlib import Path

from .audit import AuditLog, now_utc


def sha256_bytes(data: bytes) -> str:
    """Return the SHA-256 fingerprint of `data` as 64 hex characters."""
    return hashlib.sha256(data).hexdigest()


def split_records(data: bytes) -> list[bytes]:
    """Lines exactly as stored, without the trailing newline. Hashes are taken on these bytes."""
    lines = data.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    return lines


class WriteOnceError(Exception):
    """Raised when something tries to replace a raw file with different content."""
    pass


class RawZone:
    """Write-once storage for original evidence, one folder per source, with a hash manifest per file."""

    def __init__(self, root: Path, audit: AuditLog):
        """Point the raw zone at `root`. Manifests live in `root/_manifests`. Every write is logged to `audit`."""
        self.root = root
        self.audit = audit
        self.manifest_root = root / "_manifests"

    def store(self, source: str, partition: str, filename: str, data: bytes,
              records: list[dict], ingestion_path: str, **extra) -> bool:
        """Returns True if the file was written, False if identical bytes were already stored."""
        rel = Path(source) / partition / filename
        path = self.root / rel
        manifest_path = self.manifest_root / (str(rel) + ".json")
        digest = sha256_bytes(data)

        if path.exists():
            if sha256_bytes(path.read_bytes()) == digest:
                self.audit.append("raw_already_stored", file=str(rel), sha256=digest)
                return False
            self.audit.append("raw_write_refused", file=str(rel), reason="different bytes for an existing raw file")
            raise WriteOnceError(f"{rel} already exists with different content; raw files are never replaced")

        path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "xb") as f:
            f.write(data)
        manifest = {
            "file": str(rel),
            "source": source,
            "ingestion_path": ingestion_path,
            "ingested_at": now_utc(),
            "sha256": digest,
            "bytes": len(data),
            "records": records,
            **extra,
        }
        with open(manifest_path, "x") as f:
            json.dump(manifest, f, indent=2)
        for p in (path, manifest_path):
            try:
                os.chmod(p, 0o444)  # best effort; the shared project folder ignores permissions
            except OSError:
                pass
        self.audit.append("raw_stored", file=str(rel), sha256=digest, records=len(records),
                          ingestion_path=ingestion_path)
        return True

    def manifests(self) -> list[dict]:
        """Load every manifest in the raw zone, sorted by path. Returns an empty list if nothing has been stored yet."""
        if not self.manifest_root.exists():
            return []
        return [json.loads(p.read_text()) for p in sorted(self.manifest_root.rglob("*.json"))]


def load_checksums(path: Path) -> dict[str, str]:
    """Read a sha256sum-style file: '<hash>  <filename>' per line."""
    sums = {}
    if path.exists():
        for line in path.read_text().splitlines():
            if line.strip():
                h, name = line.split(maxsplit=1)
                sums[name.strip().lstrip("*")] = h
    return sums

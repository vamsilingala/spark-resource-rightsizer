"""Portable discovery and decompression for Spark event-log objects."""

from __future__ import annotations

import bz2
import contextlib
import glob
import gzip
import io
import lzma
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator, List, Tuple
from urllib.parse import urlparse

from spark_rightsizer.domain import SparkEvidence
from spark_rightsizer.event_stream import ListenerEventAccumulator


def discover_objects(location: str) -> List[str]:
    parsed = urlparse(location)
    if parsed.scheme in {"", "file"}:
        local = parsed.path if parsed.scheme == "file" else location
        candidate = Path(local).expanduser()
        if candidate.is_dir():
            return sorted(str(path) for path in candidate.rglob("*") if path.is_file())
        return sorted(path for path in glob.glob(str(candidate), recursive=True) if Path(path).is_file())

    try:
        import fsspec
    except ImportError as exc:
        raise RuntimeError("Remote event logs require the cloud-files package extra") from exc
    filesystem, object_path = fsspec.core.url_to_fs(location)
    matches = filesystem.find(object_path) if filesystem.isdir(object_path) else filesystem.glob(object_path)
    return sorted(filesystem.unstrip_protocol(path) for path in matches if not filesystem.isdir(path))


@contextlib.contextmanager
def _binary_stream(location: str) -> Iterator[BinaryIO]:
    parsed = urlparse(location)
    if parsed.scheme in {"", "file"}:
        local = parsed.path if parsed.scheme == "file" else location
        with open(local, "rb") as stream:
            yield stream
        return
    try:
        import fsspec
    except ImportError as exc:
        raise RuntimeError("Remote event logs require the cloud-files package extra") from exc
    with fsspec.open(location, "rb").open() as stream:
        yield stream


def _decoded_stream(raw: BinaryIO, location: str) -> io.TextIOBase:
    lower = location.casefold()
    if lower.endswith(".gz"):
        source: BinaryIO = gzip.GzipFile(fileobj=raw)
    elif lower.endswith(".bz2"):
        source = bz2.BZ2File(raw)
    elif lower.endswith((".xz", ".lzma")):
        source = lzma.LZMAFile(raw)
    else:
        source = raw
    return io.TextIOWrapper(source, encoding="utf-8", errors="replace")


def read_event_locations(locations: Iterable[str]) -> Tuple[SparkEvidence, List[str]]:
    accumulator = ListenerEventAccumulator()
    problems: List[str] = []
    found = 0
    for location in locations:
        try:
            objects = discover_objects(str(location))
        except Exception as exc:
            problems.append(f"{location}: discovery failed: {exc}")
            continue
        if not objects:
            problems.append(f"{location}: no Spark event-log objects found")
            continue
        for object_uri in objects:
            found += 1
            try:
                with _binary_stream(object_uri) as raw:
                    text = _decoded_stream(raw, object_uri)
                    accumulator.accept_json_lines(text)
            except Exception as exc:
                problems.append(f"{object_uri}: read failed: {exc}")
    return accumulator.result(every_file_read=found > 0 and not problems), problems

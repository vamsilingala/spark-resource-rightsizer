"""Single-pass aggregation of public Apache Spark listener events."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Tuple

from spark_rightsizer.domain import SparkEvidence
from spark_rightsizer.statistics import quantile, weighted_quantile


def _metric(container: Dict[str, Any], *keys: str) -> int:
    for key in keys:
        candidate = container.get(key)
        if isinstance(candidate, (int, float)):
            return int(candidate)
    casefolded = {str(key).casefold(): value for key, value in container.items()}
    for key in keys:
        candidate = casefolded.get(key.casefold())
        if isinstance(candidate, (int, float)):
            return int(candidate)
    return 0


@dataclass
class _ExecutorLifetime:
    opened: int
    closed: Optional[int] = None


class ListenerEventAccumulator:
    """Aggregate JSON listener records without materializing the complete log."""

    def __init__(self) -> None:
        self.events_seen = 0
        self.applications_started = 0
        self.applications_finished = 0
        self.application_keys: set = set()
        self.tasks = 0
        self.failed_tasks = 0
        self.executor_compute_ms = 0
        self.jvm_gc_ms = 0
        self.bytes_input = 0
        self.bytes_shuffle_read = 0
        self.bytes_shuffle_written = 0
        self.bytes_memory_spilled = 0
        self.bytes_disk_spilled = 0
        self.bytes_results = 0
        self.executor_rss_high_watermark = 0
        self.driver_rss_high_watermark = 0
        self.executor_lifetimes: Dict[str, _ExecutorLifetime] = {}
        self.stage_task_durations: DefaultDict[int, List[int]] = defaultdict(list)
        self.hazards: set = set()
        self.earliest_ms: Optional[int] = None
        self.latest_ms: Optional[int] = None

    def accept_json_lines(self, lines: Iterable[str]) -> None:
        for raw in lines:
            if not raw.strip():
                continue
            try:
                record = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if isinstance(record, dict):
                self.accept(record)

    def accept(self, record: Dict[str, Any]) -> None:
        self.events_seen += 1
        event_type = str(record.get("Event", ""))
        handlers = {
            "SparkListenerApplicationStart": self._application_started,
            "SparkListenerApplicationEnd": self._application_finished,
            "SparkListenerExecutorAdded": self._executor_added,
            "SparkListenerExecutorRemoved": self._executor_removed,
            "SparkListenerTaskEnd": self._task_finished,
        }
        handler = handlers.get(event_type)
        if handler is not None:
            handler(record)
        else:
            self._classify_failure(record)

    def _application_started(self, record: Dict[str, Any]) -> None:
        self.applications_started += 1
        key = record.get("App ID") or record.get("App Attempt ID")
        if key:
            self.application_keys.add(str(key))
        self._mark_time(_metric(record, "Timestamp"))

    def _application_finished(self, record: Dict[str, Any]) -> None:
        self.applications_finished += 1
        self._mark_time(_metric(record, "Timestamp"))

    def _executor_added(self, record: Dict[str, Any]) -> None:
        key = str(record.get("Executor ID", ""))
        timestamp = _metric(record, "Timestamp")
        if key and key != "driver" and timestamp > 0:
            self.executor_lifetimes[key] = _ExecutorLifetime(opened=timestamp)
        self._mark_time(timestamp)

    def _executor_removed(self, record: Dict[str, Any]) -> None:
        key = str(record.get("Executor ID", ""))
        timestamp = _metric(record, "Timestamp")
        lifetime = self.executor_lifetimes.get(key)
        if lifetime is not None:
            lifetime.closed = timestamp
        self._mark_time(timestamp)
        self._classify_failure(record.get("Removed Reason", record))

    def _task_finished(self, record: Dict[str, Any]) -> None:
        info = record.get("Task Info") if isinstance(record.get("Task Info"), dict) else {}
        measurements = record.get("Task Metrics") if isinstance(record.get("Task Metrics"), dict) else {}
        launched = _metric(info, "Launch Time")
        finished = _metric(info, "Finish Time")
        duration = max(0, finished - launched)

        self.tasks += 1
        reason = str(record.get("Task End Reason", "")).casefold()
        if bool(info.get("Failed")) or "success" not in reason:
            self.failed_tasks += 1
        if duration:
            self.stage_task_durations[_metric(record, "Stage ID")].append(duration)

        self.executor_compute_ms += _metric(measurements, "Executor Run Time")
        self.jvm_gc_ms += _metric(measurements, "JVM GC Time")
        self.bytes_memory_spilled += _metric(measurements, "Memory Bytes Spilled")
        self.bytes_disk_spilled += _metric(measurements, "Disk Bytes Spilled")
        self.bytes_results += _metric(measurements, "Result Size")
        input_values = measurements.get("Input Metrics") or {}
        read_values = measurements.get("Shuffle Read Metrics") or {}
        written_values = measurements.get("Shuffle Write Metrics") or {}
        self.bytes_input += _metric(input_values, "Bytes Read")
        self.bytes_shuffle_read += sum(
            _metric(read_values, key)
            for key in ("Remote Bytes Read", "Local Bytes Read", "Remote Bytes Read To Disk")
        )
        self.bytes_shuffle_written += _metric(written_values, "Shuffle Bytes Written")

        process_values = record.get("Task Executor Metrics") or {}
        resident_bytes = sum(
            _metric(process_values, key)
            for key in (
                "ProcessTreeJVMRSSMemory",
                "ProcessTreePythonRSSMemory",
                "ProcessTreeOtherRSSMemory",
            )
        )
        executor_key = str(info.get("Executor ID", ""))
        if executor_key == "driver":
            self.driver_rss_high_watermark = max(self.driver_rss_high_watermark, resident_bytes)
        else:
            self.executor_rss_high_watermark = max(self.executor_rss_high_watermark, resident_bytes)

        self._mark_time(launched)
        self._mark_time(finished)
        self._classify_failure(record.get("Task End Reason", {}))

    def _classify_failure(self, value: Any) -> None:
        description = json.dumps(value, default=str).casefold()
        markers = {
            "memory_exhaustion": ("outofmemory", "out of memory", "memory limit"),
            "executor_interruption": ("executor lost", "container killed", "heartbeat timed out"),
            "shuffle_fetch_failure": ("fetchfailed", "fetch failed"),
        }
        for label, phrases in markers.items():
            if any(phrase in description for phrase in phrases):
                self.hazards.add(label)

    def _mark_time(self, timestamp: int) -> None:
        if timestamp <= 0:
            return
        self.earliest_ms = timestamp if self.earliest_ms is None else min(self.earliest_ms, timestamp)
        self.latest_ms = timestamp if self.latest_ms is None else max(self.latest_ms, timestamp)

    def _executor_distribution(self) -> Tuple[Optional[float], Optional[float], Optional[int]]:
        if not self.executor_lifetimes or self.latest_ms is None:
            return None, None, None
        changes: List[Tuple[int, int]] = []
        for lifetime in self.executor_lifetimes.values():
            changes.append((lifetime.opened, 1))
            changes.append((lifetime.closed or self.latest_ms, -1))
        changes.sort(key=lambda value: (value[0], value[1]))
        active = 0
        prior = changes[0][0]
        weighted_counts: List[Tuple[float, float]] = []
        weighted_total = 0.0
        elapsed = 0
        maximum = 0
        for timestamp, delta in changes:
            interval = max(0, timestamp - prior)
            if interval:
                weighted_counts.append((float(active), float(interval)))
                weighted_total += active * interval
                elapsed += interval
            active = max(0, active + delta)
            maximum = max(maximum, active)
            prior = timestamp
        mean = weighted_total / elapsed if elapsed else float(maximum)
        return round(mean, 2), weighted_quantile(weighted_counts, 0.95), maximum

    def _stage_skew(self) -> Optional[float]:
        ratios: List[float] = []
        for values in self.stage_task_durations.values():
            median = quantile(values, 0.50)
            upper = quantile(values, 0.95)
            if median and upper is not None:
                ratios.append(upper / median)
        return max(ratios) if ratios else None

    def result(self, every_file_read: bool = True, origin: str = "spark-listener-log") -> SparkEvidence:
        mean, q95, maximum = self._executor_distribution()
        complete = bool(
            every_file_read
            and self.applications_started > 0
            and self.applications_finished >= self.applications_started
            and self.tasks > 0
        )
        application_count = len(self.application_keys) or (1 if self.tasks else 0)
        return SparkEvidence(
            is_complete=complete,
            applications=application_count,
            tasks=self.tasks,
            failed_tasks=self.failed_tasks,
            executor_compute_ms=self.executor_compute_ms,
            jvm_gc_ms=self.jvm_gc_ms,
            bytes_input=self.bytes_input,
            bytes_shuffle_read=self.bytes_shuffle_read,
            bytes_shuffle_written=self.bytes_shuffle_written,
            bytes_memory_spilled=self.bytes_memory_spilled,
            bytes_disk_spilled=self.bytes_disk_spilled,
            bytes_results=self.bytes_results,
            executor_rss_high_watermark=self.executor_rss_high_watermark,
            driver_rss_high_watermark=self.driver_rss_high_watermark,
            executor_count_mean=mean,
            executor_count_q95=q95,
            executor_count_max=maximum,
            stage_skew_q95_median=self._stage_skew(),
            hazards=sorted(self.hazards),
            origin=origin,
        )

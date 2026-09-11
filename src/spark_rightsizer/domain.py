"""Vendor-neutral data contracts used by the assessment pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class Cloud(str, Enum):
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"


class Runtime(str, Enum):
    DATABRICKS = "databricks"
    GLUE = "aws_glue"
    FILE = "file"


class ExecutionState(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    STOPPED = "stopped"
    ACTIVE = "active"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class MachineShape:
    key: str
    cores: float
    ram_gib: float
    scratch_gib: float = 0.0
    price_per_hour: Optional[float] = None
    cpu_arch: str = "managed"
    units: float = 1.0


@dataclass
class ClusterLayout:
    cloud: Cloud
    runtime: Runtime
    region: str
    worker_shape: str
    fixed_workers: Optional[int] = None
    scale_floor: Optional[int] = None
    scale_ceiling: Optional[int] = None
    driver_shape: Optional[str] = None
    platform_minimum: int = 1
    attributes: Dict[str, Any] = field(default_factory=dict)

    @property
    def uses_autoscaling(self) -> bool:
        return self.scale_floor is not None and self.scale_ceiling is not None

    @property
    def baseline_workers(self) -> Optional[int]:
        return self.scale_floor if self.uses_autoscaling else self.fixed_workers


@dataclass(frozen=True)
class Workload:
    key: str
    label: str
    principal: str = ""
    annotations: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Execution:
    key: str
    workload_key: str
    state: ExecutionState
    began_at: datetime
    finished_at: Optional[datetime]
    attempt_index: int = 0
    recovered: bool = False
    context_complete: bool = True
    layout: Optional[ClusterLayout] = None
    attributes: Dict[str, Any] = field(default_factory=dict)

    @property
    def elapsed_seconds(self) -> float:
        if self.finished_at is None:
            return 0.0
        return max(0.0, (self.finished_at - self.began_at).total_seconds())

    @property
    def eligible(self) -> bool:
        return (
            self.state == ExecutionState.SUCCESS
            and self.finished_at is not None
            and self.elapsed_seconds > 0
            and self.attempt_index == 0
            and not self.recovered
            and self.context_complete
        )


@dataclass
class HostSignals:
    observations: int = 0
    cpu_q95: Optional[float] = None
    cpu_max: Optional[float] = None
    ram_q95: Optional[float] = None
    ram_max: Optional[float] = None
    io_wait_q95: Optional[float] = None
    allocation_q95: Optional[float] = None
    origin: str = "unavailable"

    @property
    def usable(self) -> bool:
        return self.observations > 0 and any(
            value is not None for value in (self.cpu_q95, self.ram_q95, self.allocation_q95)
        )


@dataclass
class SparkEvidence:
    is_complete: bool = False
    applications: int = 0
    tasks: int = 0
    failed_tasks: int = 0
    executor_compute_ms: int = 0
    jvm_gc_ms: int = 0
    bytes_input: int = 0
    bytes_shuffle_read: int = 0
    bytes_shuffle_written: int = 0
    bytes_memory_spilled: int = 0
    bytes_disk_spilled: int = 0
    bytes_results: int = 0
    executor_rss_high_watermark: int = 0
    driver_rss_high_watermark: int = 0
    executor_count_mean: Optional[float] = None
    executor_count_q95: Optional[float] = None
    executor_count_max: Optional[int] = None
    stage_skew_q95_median: Optional[float] = None
    hazards: List[str] = field(default_factory=list)
    origin: str = "unavailable"

    @property
    def gc_share(self) -> float:
        if self.executor_compute_ms <= 0:
            return 0.0
        return min(1.0, self.jvm_gc_ms / self.executor_compute_ms)

    @property
    def shuffle_gib(self) -> float:
        return (self.bytes_shuffle_read + self.bytes_shuffle_written) / 1024**3

    @property
    def spill_to_input(self) -> float:
        if self.bytes_input <= 0:
            return 1.0 if self.bytes_disk_spilled > 0 else 0.0
        return self.bytes_disk_spilled / self.bytes_input


@dataclass
class Snapshot:
    workload: Workload
    execution: Execution
    layout: ClusterLayout
    workers: HostSignals = field(default_factory=HostSignals)
    driver: HostSignals = field(default_factory=HostSignals)
    spark: SparkEvidence = field(default_factory=SparkEvidence)
    notes: List[str] = field(default_factory=list)


@dataclass
class ChangePlan:
    operations: List[str] = field(default_factory=list)
    fixed_workers: Optional[int] = None
    scale_floor: Optional[int] = None
    scale_ceiling: Optional[int] = None
    worker_shape: Optional[str] = None
    driver_shape: Optional[str] = None
    capacity_explanation: str = ""
    worker_shape_explanation: str = ""
    driver_shape_explanation: str = ""
    assurance: str = "low"
    projected_saving_percent: Optional[float] = None
    saving_scope: str = ""

    @property
    def changes_configuration(self) -> bool:
        return bool(self.operations and self.operations != ["no_change"])

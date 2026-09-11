"""Connector contract for normalizing platform telemetry."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Iterable, Sequence

from spark_rightsizer.domain import Execution, MachineShape, Snapshot, Workload


class Connector(ABC):
    @abstractmethod
    def workloads(self) -> Iterable[Workload]:
        raise NotImplementedError

    @abstractmethod
    def executions(self, workload: Workload, start: datetime, end: datetime) -> Iterable[Execution]:
        raise NotImplementedError

    @abstractmethod
    def snapshot(self, workload: Workload, execution: Execution) -> Snapshot:
        raise NotImplementedError

    @abstractmethod
    def shape_catalog(self) -> Dict[str, MachineShape]:
        raise NotImplementedError

    @abstractmethod
    def worker_shape_choices(self) -> Sequence[str]:
        raise NotImplementedError

    @abstractmethod
    def driver_shape_choices(self) -> Sequence[str]:
        raise NotImplementedError

    def close(self) -> None:
        return None

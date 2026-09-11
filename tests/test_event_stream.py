import json

from spark_rightsizer.event_stream import ListenerEventAccumulator


def _task(launch, finish, executor="1", failed=False, reason="Success"):
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": 7,
        "Task Info": {
            "Launch Time": launch,
            "Finish Time": finish,
            "Executor ID": executor,
            "Failed": failed,
        },
        "Task End Reason": reason,
        "Task Metrics": {
            "Executor Run Time": finish - launch,
            "JVM GC Time": 2,
            "Input Metrics": {"Bytes Read": 100},
            "Shuffle Read Metrics": {"Remote Bytes Read": 20, "Local Bytes Read": 5},
            "Shuffle Write Metrics": {"Shuffle Bytes Written": 30},
            "Disk Bytes Spilled": 4,
        },
        "Task Executor Metrics": {
            "ProcessTreeJVMRSSMemory": 10,
            "ProcessTreePythonRSSMemory": 11,
            "ProcessTreeOtherRSSMemory": 12,
        },
    }


def test_listener_events_produce_complete_evidence():
    records = [
        {"Event": "SparkListenerApplicationStart", "App ID": "app-1", "Timestamp": 1000},
        {"Event": "SparkListenerExecutorAdded", "Executor ID": "1", "Timestamp": 1100},
        _task(1200, 1300),
        _task(1400, 1600),
        {"Event": "SparkListenerExecutorRemoved", "Executor ID": "1", "Timestamp": 1700},
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 1800},
    ]
    accumulator = ListenerEventAccumulator()
    accumulator.accept_json_lines(json.dumps(record) for record in records)
    evidence = accumulator.result()
    assert evidence.is_complete
    assert evidence.tasks == 2
    assert evidence.bytes_shuffle_read == 50
    assert evidence.bytes_shuffle_written == 60
    assert evidence.executor_rss_high_watermark == 33
    assert evidence.executor_count_max == 1
    assert evidence.executor_count_q95 == 1


def test_failures_are_classified_without_stopping_the_stream():
    accumulator = ListenerEventAccumulator()
    accumulator.accept(_task(1, 2, failed=True, reason="ExecutorLostFailure: container killed"))
    evidence = accumulator.result()
    assert not evidence.is_complete
    assert evidence.failed_tasks == 1
    assert "executor_interruption" in evidence.hazards

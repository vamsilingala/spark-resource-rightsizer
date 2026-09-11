import gzip
import json

from spark_rightsizer.storage import discover_objects, read_event_locations


def test_local_directory_is_recursive_and_gzip_is_decoded(tmp_path):
    nested = tmp_path / "nested"
    nested.mkdir()
    path = nested / "events.json.gz"
    records = [
        {"Event": "SparkListenerApplicationStart", "App ID": "a", "Timestamp": 1},
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": 1,
            "Task Info": {"Launch Time": 2, "Finish Time": 3, "Executor ID": "1"},
            "Task End Reason": "Success",
            "Task Metrics": {},
        },
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 4},
    ]
    with gzip.open(path, "wt") as stream:
        for record in records:
            stream.write(json.dumps(record) + "\n")
    assert discover_objects(str(tmp_path)) == [str(path)]
    evidence, problems = read_event_locations([str(tmp_path)])
    assert evidence.is_complete
    assert evidence.tasks == 1
    assert problems == []


def test_missing_location_is_reported(tmp_path):
    evidence, problems = read_event_locations([str(tmp_path / "missing")])
    assert not evidence.is_complete
    assert "no Spark event-log objects" in problems[0]

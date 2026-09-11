import json
from pathlib import Path

from spark_rightsizer.cli import main


def test_offline_cli_runs_end_to_end(tmp_path, monkeypatch, capsys):
    project = Path(__file__).parents[1]
    data = json.loads((project / "configs" / "file.example.json").read_text())
    data["source"]["parameters"]["snapshot_file"] = str(
        project / "examples" / "sample_snapshot.json"
    )
    data["output"]["directory"] = str(tmp_path / "results")
    config = tmp_path / "config.json"
    config.write_text(json.dumps(data))
    monkeypatch.chdir(tmp_path)
    code = main(["--config", str(config)])
    output = capsys.readouterr().out
    assert code == 0
    assert "workloads=1 selections=1 assessments=1 issues=0" in output

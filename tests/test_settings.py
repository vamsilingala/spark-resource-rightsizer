import json

import pytest

from spark_rightsizer.domain import Cloud, Runtime
from spark_rightsizer.settings import load_settings


def test_json_settings_are_loaded(tmp_path):
    path = tmp_path / "settings.json"
    path.write_text(
        json.dumps(
            {
                "source": {"kind": "file", "cloud": "gcp", "region": "sample"},
                "window": {"end": "2026-01-02", "days": 9, "timezone": "UTC"},
                "policy": {"minimum_host_observations": 20},
                "shapes": {"small": {"cores": 4, "ram_gib": 16}},
            }
        )
    )
    settings = load_settings(path)
    assert settings.source.kind == Runtime.FILE
    assert settings.source.cloud == Cloud.GCP
    assert settings.history_days == 9
    assert settings.policy.minimum_host_observations == 20
    assert settings.shapes["small"].ram_gib == 16


def test_glue_rejects_non_aws_cloud(tmp_path):
    path = tmp_path / "settings.json"
    path.write_text(
        json.dumps({"source": {"kind": "aws_glue", "cloud": "azure", "region": "sample"}})
    )
    with pytest.raises(ValueError, match="only be paired"):
        load_settings(path)


def test_unknown_policy_field_is_rejected(tmp_path):
    path = tmp_path / "settings.json"
    path.write_text(
        json.dumps(
            {
                "source": {"kind": "file", "cloud": "aws", "region": "sample"},
                "policy": {"invented_setting": 1},
            }
        )
    )
    with pytest.raises(ValueError, match="Unknown policy"):
        load_settings(path)

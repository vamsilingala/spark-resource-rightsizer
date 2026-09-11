from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import List, Optional

from spark_rightsizer.connectors import make_connector
from spark_rightsizer.export import write_assessments
from spark_rightsizer.settings import load_settings
from spark_rightsizer.workflow import AssessmentWorkflow


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="spark-rightsizer",
        description="Assess Spark capacity through a provider-neutral workflow.",
    )
    parser.add_argument("--config", required=True, help="YAML or JSON configuration file")
    parser.add_argument("--print-errors", action="store_true", help="Print non-fatal errors as JSON")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    connector = None
    try:
        settings = load_settings(args.config)
        connector = make_connector(settings)
        result = AssessmentWorkflow(settings, connector).execute()
    except Exception as exc:
        print(f"spark-rightsizer: {exc}", file=sys.stderr)
        return 2
    finally:
        if connector is not None:
            connector.close()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    paths = write_assessments(result.rows, settings.report_directory, stamp)
    print(
        f"workloads={result.workloads_seen} selections={result.executions_selected} "
        f"assessments={len(result.rows)} issues={len(result.issues)}"
    )
    print(f"csv={paths['csv']}")
    print(f"json={paths['json']}")
    if args.print_errors and result.issues:
        print(json.dumps(result.issues, indent=2))
    return 0 if result.rows else 2


if __name__ == "__main__":
    sys.exit(main())

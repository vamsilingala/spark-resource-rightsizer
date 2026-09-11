"""Connector construction."""

from spark_rightsizer.connectors.base import Connector
from spark_rightsizer.domain import Runtime
from spark_rightsizer.settings import Settings


def make_connector(settings: Settings) -> Connector:
    if settings.source.kind == Runtime.FILE:
        from spark_rightsizer.connectors.file import OfflineConnector

        return OfflineConnector(settings)
    if settings.source.kind == Runtime.DATABRICKS:
        from spark_rightsizer.connectors.databricks import DatabricksConnector

        return DatabricksConnector(settings)
    if settings.source.kind == Runtime.GLUE:
        from spark_rightsizer.connectors.glue import GlueConnector

        return GlueConnector(settings)
    raise ValueError("Unsupported source kind: " + settings.source.kind.value)

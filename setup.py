"""Compatibility shim for editable installs with older pip/setuptools releases."""

from setuptools import find_packages, setup

setup(
    name="spark-job-rightsizer",
    version="0.2.0",
    description="Independent provider-neutral Spark capacity assessment toolkit",
    python_requires=">=3.11",
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=["PyYAML>=6.0,<7"],
    extras_require={
        "databricks": [
            "databricks-sdk>=0.40,<1",
            "databricks-sql-connector>=3,<5",
        ],
        "aws-glue": ["boto3>=1.34,<2"],
        "cloud-files": ["fsspec>=2024.12,<2027"],
        "aws-files": [
            "fsspec>=2024.12,<2027",
            "s3fs>=2024.12,<2027",
        ],
        "azure-files": [
            "fsspec>=2024.12,<2027",
            "adlfs>=2024.12,<2027",
        ],
        "gcp-files": [
            "fsspec>=2024.12,<2027",
            "gcsfs>=2024.12,<2027",
        ],
        "dev": ["pytest>=8", "ruff>=0.6"],
    },
    entry_points={
        "console_scripts": ["spark-rightsizer=spark_rightsizer.cli:main"],
    },
)

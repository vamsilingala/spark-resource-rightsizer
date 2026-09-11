# Support matrix

| Capability | Databricks AWS | Databricks Azure | Databricks GCP | AWS Glue Spark | Offline |
|---|---:|---:|---:|---:|---:|
| Workload and execution inventory | Yes | Yes | Yes | Yes | From file |
| Host CPU and RAM | Compute table | Compute table | Compute table | CloudWatch | From file |
| Spark listener logs | S3/local | ADLS/local | GCS/local | S3 | From file |
| Fixed and autoscaling layouts | Yes | Yes | Yes | Yes | Yes |
| Worker-shape proposal | Configured catalog | Configured catalog | Configured catalog | Built-in catalog | Configured catalog |
| Driver-shape proposal | Yes | Yes | Yes | Not separate | As supplied |

Native Amazon EMR, Google Dataproc, Azure Synapse, and HDInsight inventory connectors are not included. Their
data can be assessed after translation to the offline snapshot contract.

AWS Glue Python shell and Ray jobs, Databricks serverless compute, automatic production changes, and live price
discovery are outside the current scope.

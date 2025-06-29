⚠️ Redshift Connector Versioning Issue (and Resolution)
While integrating AWS Glue with Amazon Redshift, we initially used the latest version of the redshift-connector Python library. However, this version came bundled with newer dependencies like botocore and awscli, which conflicted with the pre-installed packages in the AWS Glue environment, causing runtime errors.

Resolution
To resolve this, we switched to an earlier version of redshift-connector (2.0.888), which does not include external dependencies. This made it fully compatible with the AWS Glue Python shell environment out of the box. We uploaded the .whl file of that version to S3 and specified its path in the Glue job’s Python library path setting.

This allowed us to successfully connect to Redshift, execute SQL queries, and load data without dependency conflicts.

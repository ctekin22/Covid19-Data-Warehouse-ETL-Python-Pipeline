## ⚠️ Redshift Connector Versioning Issue (and Resolution)

While integrating AWS Glue with Amazon Redshift, we initially used the latest version of the `redshift-connector` Python library. However, this version introduced dependency conflicts due to bundled packages like `botocore` and `awscli`, which clashed with the pre-installed libraries in the AWS Glue environment — causing runtime errors.

### Resolution

We resolved the issue by switching to a dependency-free version: `redshift-connector==2.0.888`. This version does **not** bundle any external dependencies, making it compatible with AWS Glue's Python shell environment.

To apply this fix:

1. Download the `.whl` file for version `2.0.888` from [PyPI](https://pypi.org/project/redshift-connector/2.0.888/#files).
2. Upload the `.whl` file to an S3 bucket.
3. In your Glue job, go to **Job Parameters → Python library path** and provide the S3 URI to the uploaded `.whl` file.

This allowed us to connect to Redshift and execute data loading without errors.

> 🔗 Tip: Always verify the compatibility of external Python packages with the Glue version you're using to avoid hidden conflicts.


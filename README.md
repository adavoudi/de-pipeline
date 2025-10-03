# Cloud Credential Setup

This project can push generated CSV files directly to S3, Google Cloud Storage, or Azure Blob Storage by supplying the `--output-uri` flag to `data_generators/ad_spend_generator.py`. Configure credentials for each cloud provider before running the generator.

## Common Requirements
- Install the optional SDKs you need: `boto3` for AWS, `google-cloud-storage` for GCP, and `azure-storage-blob` for Azure.
- Ensure the account you use has permission to upload objects to the target bucket or container.

## Amazon S3
The generator relies on `boto3`, which follows the standard AWS credential chain. You can authenticate by either method below:
- Environment variables:
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_SESSION_TOKEN` (if using temporary credentials)
  - Optional `AWS_DEFAULT_REGION` or set a default in your shared config
- Profile-based auth: run `aws configure` to populate `~/.aws/credentials` and `~/.aws/config`, or export `AWS_PROFILE` with the name of an existing profile.

Confirm access with `aws s3 ls s3://your-bucket` before running:

```bash
python data_generators/ad_spend_generator.py --date 2025-09-29 --output-uri s3://your-bucket/path/
```

## Google Cloud Storage
`google-cloud-storage` uses Application Default Credentials (ADC).

1. Create or select a service account with `Storage Object Admin` (or more restrictive equivalent).
2. Download the JSON key and set `GOOGLE_APPLICATION_CREDENTIALS` to its path:

   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
   ```

   Alternatively, run `gcloud auth application-default login` to obtain user credentials.
3. Ensure the environment has a `GOOGLE_CLOUD_PROJECT` set if the default project is not embedded in the credentials.

Test the upload:

```bash
python data_generators/ad_spend_generator.py --date 2025-09-29 --output-uri gs://your-bucket/path/
```

## Azure Blob Storage
`azure-storage-blob` accepts either a connection string or an account key/SAS token.

- **Connection string (recommended):**

  ```bash
  export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=..."
  ```

- **Account key:**

  ```bash
  export AZURE_STORAGE_KEY="<account-key>"
  ```

- **SAS token:**

  ```bash
  export AZURE_STORAGE_SAS_TOKEN="?sv=..."
  ```

When using account keys or SAS tokens, make sure the storage account name is part of the `azure://` URI (e.g., `azure://mystorage/container/prefix`). Verify the container exists or allow the account permissions to create it.

Example invocation:

```bash
python data_generators/ad_spend_generator.py --date 2025-09-29 --output-uri azure://mystorage/my-container/prefix/
```

## Troubleshooting
- Missing SDKs raise runtime errors telling you which package to install.
- Permission errors often manifest as HTTP 403/AccessDenied responses; double-check role assignments or bucket/container ACLs.
- For proxy or firewall-restricted environments, configure the relevant proxy variables (`HTTP_PROXY`, `HTTPS_PROXY`).


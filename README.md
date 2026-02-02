# Gata Data Prep

Prepares machine learning training datasets from Zendesk ticket data for Gata Router. This container extracts ticket data from an AWS RDS database, processes it according to configurable rules, and generates stratified train/test datasets stored in Amazon S3.

## Purpose

This component is part of the larger [Gata Router](https://gata.works) project, which provides AI-powered ticket routing for customer support teams. The data prep container:

- Extracts historical Zendesk ticket data from RDS
- Applies label filtering and low-volume handling
- Creates stratified train/test splits (90/10)
- Generates two datasets: general routing and low-volume labels
- Uploads formatted datasets to S3 for model training

## Features

- **Batch Processing**: Process data by time period using batch IDs
- **Low-Volume Handling**: Automatically identifies and handles labels low volumes
- **Dual Dataset Creation**: Generates both general and low-volume specialized datasets
- **Stratified Splitting**: Maintains label distribution across train/test sets
- **Fail-Fast Philosophy**: Minimal error handling to surface underlying issues immediately

## Prerequisites

- Python 3.14
- Docker (for containerized deployment)
- AWS Resources:
  - RDS Aurora Serverless cluster with ticket data
  - S3 bucket for output datasets
  - AWS Secrets Manager secret for database credentials
  - IAM permissions for RDS Data API, S3, and Secrets Manager

The AWS resources are provisioned by the [Gata Terraform module](https://github.com/gata-router/terraform-aws-gata).

## Configuration

The application is configured via environment variables. Copy `example.env` to `local.env` and configure:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DB_ARN` | Yes | - | ARN of the Aurora Serverless RDS cluster |
| `DB_SECRET_ARN` | Yes | - | ARN of the Secrets Manager secret containing database credentials for the user, not the administrator |
| `TARGET_BUCKET` | Yes | - | S3 bucket name for output datasets (without s3:// prefix) |
| `LOW_VOLUME_FALLBACK_LABEL` | Yes | - | Zendesk label ID to use when not enough low-volume labels for separate dataset |
| `AWS_DEFAULT_REGION` | No | us-east-1 | AWS region for services. Currently us-east-1 is the only supported region |
| `LOG_LEVEL` | No | INFO | Python logging level (DEBUG, INFO, WARNING, ERROR) |
| `TRAINING_PERIOD` | No | 90 | Number of days of data to include in general dataset |
| `LOW_VOLUME_PERIOD` | No | 365 | Number of days of data to include for low-volume labels |
| `LOW_VOLUME_THRESHOLD` | No | 0.033 | Percentage threshold (0.033 = 3.3%) below which labels are considered low-volume |

### AWS Secrets Manager Format

The database secret should contain:
```json
{
  "dbname": "your_database_name",
  "username": "your_username",
  "password": "your_password"
}
```

## Usage

### Batch ID Format

The batch ID must be in `YYYYMMDDHH` format (10 digits):
- `YYYY`: 4-digit year
- `MM`: 2-digit month (01-12)
- `DD`: 2-digit day (01-31)
- `HH`: 2-digit hour (00-23)

Example: `2026010203` represents January 2, 2026 at 03:00 (3:00 AM)

### Local Development

1. Install dependencies:
   ```bash
   uv sync
   ```

2. Set up environment variables:
   ```bash
   cp example.env local.env
   # Edit local.env with your configuration
   ```

3. Run the data preparation:
   ```bash
   python prepare.py 2026010203
   ```

### Docker Deployment

1. Build the container:
   ```bash
   docker build -t gata-data-prep .
   ```

2. Configure environment:
   ```bash
   cp example.env local.env
   # Edit local.env with your configuration
   ```

3. Run with environment file:
   ```bash
   docker run --env-file local.env gata-data-prep 2026010203
   ```

   Or with individual environment variables:
   ```bash
   docker run \
     -e DB_ARN="arn:aws:rds:us-east-1:123456789012:cluster:..." \
     -e DB_SECRET_ARN="arn:aws:secretsmanager:us-east-1:123456789012:secret:..." \
     -e TARGET_BUCKET="my-training-data-bucket" \
     -e LOW_VOLUME_FALLBACK_LABEL="123456" \
     gata-data-prep 2026010203
   ```

## How It Works

### Data Processing Pipeline

1. **Extraction**: Fetches ticket data from RDS for the specified time period
2. **Label Analysis**: Counts label distribution and identifies low-volume labels
3. **General Dataset**:
   - Excludes or reassigns low-volume labels
   - Performs stratified 90/10 train/test split
   - Saves to `s3://TARGET_BUCKET/training/gata-general/{batch_id}/train/data.json`
   - Saves to `s3://TARGET_BUCKET/training/gata-general/{batch_id}/test/data.json`
4. **Low-Volume Dataset** (if ≥2 labels have ≥10 records):
   - Extracts data for a longer time period (default 365 days)
   - Filters to only low-volume labels with sufficient data
   - Performs stratified 90/10 train/test split
   - Saves to `s3://TARGET_BUCKET/training/gata-low-vol/{batch_id}/train/data.json`
   - Saves to `s3://TARGET_BUCKET/training/gata-low-vol/{batch_id}/test/data.json`

### Low-Volume Label Handling

Labels representing less than the configurable threashold (default 3.3%) of the total volume are considered "low-volume." The system handles them in two ways:

- **Sufficient Data**: If at least 2 low-volume labels have ≥10 records, creates a separate specialized dataset and reassigns low-volume labels to the `LOW_VOLUME_FALLBACK_LABEL`
- **Insufficient Data**: Removes the records from the dataset.

This approach allows the model to learn both common routing patterns and handle rare but important ticket categories.

### Output Format

Datasets are saved as JSON-L files with the following structure:
```json
[
  {
    "text": "ticket subject and body text",
    "label": "123456"
  },
  ...
]
```

## Development

### Running Tests

```bash
# Run all tests
uv run coverage run -m pytest

# Review coverage report
uv run coverage report -m
```

### Code Quality

The project uses Ruff for linting and formatting:
```bash
uv run ruff check
uv run ruff format
```

### Project Structure

```
├── prepare.py          # Main entry point
├── src/
│   ├── dataset.py      # Dataset manipulation (split, filter, transform)
│   ├── db.py          # RDS Data API client
│   ├── tickets.py     # Ticket data extraction
│   └── utils.py       # Helper functions (S3, datetime, counting)
├── tests/             # Unit tests
├── Dockerfile         # Multi-stage container build
└── pyproject.toml     # Python project configuration
```

## Error Handling Philosophy

This tool is designed to **fail spectacularly** when problems occur, with minimal error handling. This is intentional:

- Forces underlying issues (bad data, misconfiguration, API problems) to be addressed
- Prevents silent failures that could corrupt training data
- Makes debugging faster by providing clear stack traces
- Reduces complexity and maintenance burden

When the tool fails, investigate and fix the root cause rather than working around it.

## IAM Permissions

When running the container locally, the execution environment needs the following AWS permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "rds-data:ExecuteStatement",
        "rds-data:BatchExecuteStatement"
      ],
      "Resource": "arn:aws:rds:REGION:ACCOUNT:cluster:CLUSTER_NAME"
    },
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:REGION:ACCOUNT:secret:SECRET_NAME"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectAcl"
      ],
      "Resource": "arn:aws:s3:::BUCKET_NAME/training/*"
    }
  ]
}
```

## Troubleshooting

### Connection Issues

If you see RDS connection errors, the Aurora Serverless cluster may be paused. The client attempts to reconnect up to 10 times over ~1 minute to allow the cluster to resume.

### No Data Found

Check that:
- The batch ID is correct and within the range of available data
- The database contains ticket data for the specified period
- The RDS credentials have SELECT permissions on the ticket tables

### Low-Volume Dataset Not Created

This is expected when fewer than 2 labels have at least 10 records below the threshold. The log will show: "Not enough low volume labels with at least 10 records to create a low volume dataset"

### Memory Issues

For very large datasets, consider:
- Reducing the `TRAINING_PERIOD` or `LOW_VOLUME_PERIOD`
- Increasing container memory allocation
- Processing in smaller batch increments

## Getting Help

If you encounter issues specific to the data preparation process, please open an issue in this repository. For questions about how this component integrates with the broader Gata Router platform, check out the main project documentation at https://gata.works.
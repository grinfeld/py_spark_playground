# Spark Iceberg dbt App

This dbt project demonstrates reading data from S3/MinIO parquet files, staging it in Iceberg tables, and creating analytical models.

## Project Structure

```
dbt/app/
├── models/
│   ├── staging/
│   │   └── stg_customers.sql          # Stage raw customer data
│   ├── marts/
│   │   └── company_employee_count.sql # Company employee aggregation
│   └── schema.yml                     # Data documentation and tests
├── examples/
│   └── customers.csv                  # Sample data
├── dbt_project.yml                    # Project configuration
├── packages.yml                       # dbt packages
└── README.md                          # This file
```

## Data Flow

1. **Raw Data**: Parquet files in S3/MinIO containing customer data (location: `{{ env_var('RAW_DATA_PATH') }}`)
2. **Staging**: `stg_customers` - Clean and validate customer data, store in Iceberg table
3. **Marts**: `company_employee_count` - Aggregate employee count by company, store as parquet

## Models

### Staging Models

- **stg_customers**: 
  - Reads from raw parquet files
  - Adds data quality checks (email validation)
  - Creates derived fields (full_name, parsed dates)
  - Stores in Iceberg table format

### Mart Models

- **company_employee_count**:
  - Aggregates employee count by company
  - Orders by number of employees (descending)
  - Stores result as parquet file in different S3 path

## Usage

### 1. Set Environment Variables

```bash
export STORAGE_BUCKET=your-bucket-name
export CATALOG_WAREHOUSE_NAME=your-warehouse
export RAW_DATA_PATH=s3a://your-bucket/raw/customers
# ... other required environment variables
```

### 2. Choose Profile

Update `dbt_project.yml` to use the appropriate profile:
- `hadoop` - For S3/MinIO with Hadoop catalog
- `glue` - For AWS Glue catalog
- `hive` - For Hive metastore catalog

### 3. Run dbt

```bash
# Install packages
dbt deps

# Seed sample data (optional)
dbt seed

# Run models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Configuration

The project uses environment variables for configuration:
- `STORAGE_BUCKET`: S3/MinIO bucket name
- `CATALOG_WAREHOUSE_NAME`: Warehouse path
- `RAW_DATA_PATH`: Complete path to raw parquet files (e.g., `s3a://bucket/raw/customers`)
- Other variables as defined in the profiles

## Data Sources

The project expects customer data in parquet format with the following schema:
- Index (integer)
- Company (string)
- First_Name (string)
- Last_Name (string)
- Email (string)
- Phone (string)
- Address (string)
- City (string)
- State (string)
- Zip_Code (string)
- Country (string)
- Job_Title (string)
- Department (string)
- Salary (integer)
- Start_Date (string, YYYY-MM-DD format)

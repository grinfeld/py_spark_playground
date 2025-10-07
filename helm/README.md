# Helm Deployment Script - my_helm.sh

## Overview

`my_helm.sh` is a helper script for setting up and managing the current project on Kubernetes. 
It orchestrates the deployment of required services for running the Airflow jobs on Kubernetes. 
Ut installs required components like Spark Operator, Apache Airflow, MinIO (object storage), and PostgreSQL database using Helm charts.

It works, currently, with macOS only. But it's easy to make adjustment for running it on Linux (and maybe on Windows).

## Features

- **Docker Image Building**: Build and push custom Docker images for the platform components
- **Initial Setup**: Deploy the entire stack from scratch with a single command
- **Service Management**: Upgrade individual services without affecting others
- **Helm Environment Configuration**: Optionally configure project-specific Helm directories

## Prerequisites

Before using this script, ensure you have:

1. **Python** - Python installed
2. **Docker** - Docker installed and running (if building images)
3. **Docker-Desktop** - Docker Desktop installed and running (if building images)
   * __Note:__ to use minikube you need to install some local docker repository (ask Google/ChatGPT how to) and change the image parameters in all yamls, scripts and code to use it, as follows `image: localhost:5050/py-spark-spark:some-tag` 
4. **Access to Kubernetes Cluster** - Valid kubeconfig with appropriate permissions

## Usage

### Basic Syntax

```bash
./my_helm.sh [OPTIONS]
```

__Note:__ to avoid problems with staff I haven't tested, better to run script inside the `helm/` directory :).

It's better

### Required Arguments

- **`--tag=<version>`** or **`-t=<version>`**  
  **Mandatory** __when using --build option or during initial setup__. Specifies the version tag for Docker images and Helm deployments.  
  Example: `--tag=v1.0.0`

### Optional Arguments

#### Common Options

- **`--build`**  
  Build and push Docker images before deployment.  
  Default: `false`. Requires **`--tag=<version>`** or **`-t=<version>`** argument.

#### Initial Setup Options

Usually, initial setup should be running only once to create k8s cluster and install all the relevant staff. All other changes better to perform according to [Service Upgrade Options](./README.md#Service-Upgrade-Options).
__Note:__ Running initial setup will override anything you have added manually previously in `service/values.yaml`. 

**ALERT** executing without option `--apply`, considered initial setup. You have been warned.

- **`--base-dir=<path>`** or **`--b-dir=<path>`**  
  Specify the base directory where Helm scripts and files are located.  
  Default: Current directory + `/helm/`

- **`--namespace=<name>`** or **`-n=<name>`**  
  Specify the Kubernetes namespace for deployment.  
  Default: `py-spark`

- **`--change-helm`**  
  Configure Helm environment variables in your `~/.zshrc` for project-specific Helm cache/config.  
  **Warning**: Only use this if running different k8s clusters per project. Not recommended!.

#### Service Upgrade Options

After the initial setup for making changes in k8s, the better option is editing `service/values.yaml` where the `service` is one of following: `minio`, `postgres`, `airflow`, `spark`.

- **`--apply`**  
  Upgrade an existing Helm release instead of performing initial setup.  
  Must be used with `--service`.

- **`--service=<name>`** or **`-s=<name>`**  
  Specify which service to upgrade when using `--apply`.  
  Valid values: `minio`, `postgres`, `airflow`, `spark`

__Note__: when using `--apply` it doesn't change rbac and so on, so if you need to make changes in rbac, do it manually.

#### Help

- **`--help`** or **`-h`**  
  Display help information.

#### Notes

The names of `services` used in script in some cases are different from those appears in the helm. Here the list of helm elements installed by `my_helm.sh`:
---------------------------------
| **my_helm name** | **helm release name** |
|------------------|-----------------------|
| postgres         | postgres-release      |
| minio            | minio-release         |
| airflow          | airflow               |
| spark            | spark                 |

## Usage Examples

### Example 1: Initial Setup (Complete Stack Deployment)

Deploy the entire platform with version v1.0.0, building Docker images:

```bash
# install required dependencies, build required images and creates and apply helm. Use default helm and default namespace (a.k.a. py-spark)
./my_helm.sh --tag=v1.0.0 --build
```

This will:
1. Install required dependencies
2. Build and push Docker images with tag v1.0.0
3. Create namespace `py-spark` (if not exists)
4. Deploy PostgreSQL
5. Deploy MinIO
6. Deploy Spark Operator
7. Deploy Apache Airflow

### Example 2: Initial Setup Without Building Images

Deploy using pre-built images:

```bash
./my_helm.sh --tag=v1.0.0
```

### Example 3: Initial Setup with Custom Namespace

Deploy to a custom namespace:

```bash
./my_helm.sh --tag=v1.0.0 --namespace=my-data-platform
```

### Example 4: Upgrade Airflow Service

Upgrade only the Airflow service after modifying its values:

```bash
./my_helm.sh --tag=v1.0.0 --apply --service=airflow
```

### Example 5: Upgrade MinIO Service

Upgrade MinIO (the script will automatically delete the post-deploy job first):

```bash
./my_helm.sh --tag=v1.0.0 --apply --service=minio
```

### Example 6: Upgrade Spark Operator

```bash
./my_helm.sh --tag=v1.0.0 --apply --service=spark
```

### Example 7: Upgrade PostgreSQL

```bash
./my_helm.sh --tag=v1.0.0 --apply --service=postgres
```

## Service Components

The script manages the following services:

### 1. PostgreSQL
- **Purpose**: Database backend for Airflow metadata
- **Helm Chart**: Custom chart in `postgres/` directory
- **Configuration**: Edit `postgres/values.yaml`

### 2. MinIO
- **Purpose**: S3-compatible object storage for data and logs
- **Helm Chart**: Custom chart in `minio/` directory
- **Configuration**: Edit `minio/values.yaml`
- **Note**: Includes post-deployment jobs for initial setup

### 3. Spark Operator
- **Purpose**: Kubernetes operator for Apache Spark applications
- **Helm Chart**: Official spark-operator chart
- **Configuration**: Edit `spark/values.yaml`

### 4. Apache Airflow
- **Purpose**: Workflow orchestration platform
- **Helm Chart**: Official Apache Airflow chart
- **Configuration**: Edit `airflow/values.yaml`

## Directory Structure

```
helm/
├── my_helm.sh              # Main deployment script
├── README.md               # This file
├── airflow/                # Airflow Helm values
│   └── values.yaml
├── minio/                  # MinIO Helm chart
│   ├── Chart.yaml
│   ├── values.yaml
│   └── templates/
├── postgres/               # PostgreSQL Helm chart
│   ├── Chart.yaml
│   ├── values.yaml
│   └── templates/
├── spark/                  # Spark Operator Helm values
│   └── values.yaml
├── scripts/                # Helper scripts
│   ├── functions.sh
│   ├── create_postgres.sh
│   ├── create_minio.sh
│   ├── create_spark.sh
│   └── create_airflow.sh
└── templates/              # Kubernetes templates
```

## Workflow

### Initial Setup Workflow

When you run `my_helm.sh` without `--apply`:

1. **Install Dependencies** - Installs required tools and validates environment
2. **Update Helm Variables** - Configures Helm environment (if `--change-helm` specified)
3. **Build Images** - Builds Docker images (if `--build` specified)
4. **Create Namespace** - Creates Kubernetes namespace if it doesn't exist
5. **Deploy Services** - Deploys services in order:
   - PostgreSQL
   - MinIO
   - Spark Operator
   - Apache Airflow

### Upgrade Workflow

When you run `my_helm.sh` with `--apply`:

1. **Validate Service Name** - Ensures service name is provided and valid
2. **Execute Service-Specific Upgrade** - Runs `helm upgrade` for the specified service
3. **Wait for Completion** - Waits for the upgrade to complete successfully

## Configuration

### Modifying Service Configuration

To modify configuration for any service:

1. Edit the corresponding `values.yaml` file:
   - Airflow: `airflow/values.yaml`
   - MinIO: `minio/values.yaml`
   - PostgreSQL: `postgres/values.yaml`
   - Spark: `spark/values.yaml`

2. Apply the changes:
   ```bash
   ./my_helm.sh --tag=<your-tag> --apply --service=<service-name>
   ```

### Image Tags

The `--tag` parameter is used to specify which version of custom Docker images to deploy. Ensure your images are built and available in your container registry with the specified tag.

## Troubleshooting

### Common Issues

#### Issue: "You should set --tag (or -t)"
**Solution**: The `--tag` parameter is mandatory. Always specify a version tag:
```bash
./my_helm.sh --tag=v1.0.0
```

#### Issue: "When applying helm for specific service, you should set service name"
**Solution**: When using `--apply`, you must specify a service:
```bash
./my_helm.sh --tag=v1.0.0 --apply --service=airflow
```

#### Issue: "Service should be one of the following names: minio, postgres, airflow, spark"
**Solution**: Use only valid service names:
- `minio`
- `postgres`
- `airflow`
- `spark`

#### Issue: Namespace already exists
**Behavior**: The script will detect existing namespaces and continue without error.

#### Issue: Helm release already exists
**Solution**: Use `--apply` to upgrade an existing release instead of trying initial setup:
```bash
./my_helm.sh --tag=v1.0.0 --apply --service=<service-name>
```

### Debugging

To debug issues:

1. **Check Kubernetes cluster connectivity**:
   ```bash
   kubectl cluster-info
   kubectl get nodes
   ```

2. **Verify namespace**:
   ```bash
   kubectl get namespace py-spark
   ```

3. **Check Helm releases**:
   ```bash
   helm list --namespace py-spark
   ```

4. **Check pod status**:
   ```bash
   kubectl get pods --namespace py-spark
   ```

5. **View logs**:
   ```bash
   kubectl logs <pod-name> --namespace py-spark
   ```

## Important Notes

1. **Initial Setup vs Upgrade**: You cannot combine initial setup and apply operations. Use one or the other.

2. **Service Dependencies**: The initial setup deploys services in a specific order to handle dependencies:
   - PostgreSQL must be running before Airflow
   - MinIO should be available for Spark and Airflow storage

3. **Helm Environment Variables**: The `--change-helm` option modifies your `~/.zshrc` file. Use with caution and only if you understand the implications.

4. **MinIO Post-Deploy Job**: When upgrading MinIO, the script automatically deletes the `minio-post-deploy-job` to allow it to be recreated.

5. **Mac-Specific**: The `--change-helm` option currently only supports macOS (modifies `~/.zshrc`).

## Additional Resources

- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Helm Documentation](https://helm.sh/docs/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Kubeflow Spark Operator Documentation](https://www.kubeflow.org/docs/components/spark-operator/overview/)
- [MinIO Documentation](https://min.io/docs/)

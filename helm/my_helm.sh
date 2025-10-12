#!/bin/bash

TAG=""
HELM_BASE_DIR=""
BUILD_IMAGE="false"
CHANGE_HELM="false"
APPLY="false"
SERVICE=""
NAMESPACE="py-spark"

print_help() {
  echo -e "--help ('-h') \n\tDisplays help."
  echo -e "--tag ('-t') \n\tThis argument is mandatory. Tag to build image or what tag to use when building helm. \n\tFor example, './my_helm.sh --build --tag=v1' will create and push images with version v1."
  echo -e "--build \n\tBuild the docker images required for this project. If set, e.g. './my_helm.sh --build', images will be built, else false and it's a default value, the images won't be built."
  echo -e "--base-dir ('-b'). \n\tHelm base dir the directory where '--base-dir=xxxxx' defines the base dir for project where the whole helm scripts, files are placed. \n\tBy default it's current project + 'helm/' folder."
  echo -e "--change-helm \n\tIf you want your helm cache/config/etc to be defined under this project folder. \n\tIf set, e.g. './my_helm.sh --change-helm' they HELM env variables will be added into your '~/.zshrc' (sorry, Mac only now :) ). \n\t Using this option is not recommended, only if you run different k8s cluster per project. \n\t Note: If you have different and/or more than one projects with helm/k8s, don't use this option, just relay on default helm directory or whatever you already use"
  echo -e "\nAfter initial built, you'll be able to change any of your configuration by editing 'helm/[service name]/values.yaml'. You can't to combine initial setup and apply operation.\n"
  echo -e "--apply \n\t Signaling not to perform initial setup but upgrade the existed helm. \n\tIt comes with conjunction with following --service argument. \n"
  echo -e "--service ('-s') \n\t Setting service to be upgraded by '--apply' operation. \n\t The usage: './my_helm.sh --apply -s=[service name]'. \n\tThe service name couldn't be empty and limited to one of the followed: minio, postgres, airflow, spark, strimzi, kafka, kafka-connect, kafka-ui, fake."
}

# Parse named arguments
while [ "$#" -gt 0 ]; do
  case "$1" in
    -n=*)
      NAMESPACE="${1#*=}"
      ;;
    --namespace=*)
      NAMESPACE="${1#*=}"
      ;;-t=*)
      TAG="${1#*=}"
      ;;
    --tag=*)
      TAG="${1#*=}"
      ;;
    --b-dir=*)
      HELM_BASE_DIR="${1#*=}"
      ;;
    --base-dir=*)
      HELM_BASE_DIR="${1#*=}"
      ;;
    --build)
      BUILD_IMAGE="true"
      ;;
    --change-helm)
      CHANGE_HELM="true"
      ;;
    --apply)
      APPLY="true"
      ;;
    --service=*)
      SERVICE="${1#*=}"
      ;;
    -s=*)
      SERVICE="${1#*=}"
      ;;
    --help)
      print_help
      exit
      ;;
    -h)
      print_help
      exit
      ;;
    *)
      echo "Following parameters are supported"
      echo -e "--help ('-h') \n\tDisplays help."
      exit 1
      ;;
  esac
  shift
done

if [[ "$HELM_BASE_DIR" == "" ]]; then
  HELM_BASE_DIR=$(pwd)
fi

echo "Using $HELM_BASE_DIR as base dir"
echo "Build image=$BUILD_IMAGE with tag $TAG"
echo "Build helm basic environments is $CHANGE_HELM"

source ./scripts/functions.sh

install_dependencies

if [[ "$CHANGE_HELM" == "" ]]; then
  update_helm_vars
fi

if [[ "$BUILD_IMAGE" == "true" ]]; then
  if [[ "$TAG" == "" ]]; then
    echo "You should set --tag (or -t), since it's mandatory argument when you build image or during initial setup. We build the helm values based on tag. Use 'my_helm.sh -h' to see help "
    exit
  fi
  build_image "$TAG"
fi

if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    echo "Creating namespace $NAMESPACE"
    kubectl create namespace "$NAMESPACE"
else
    echo "Namespace $NAMESPACE already exists"
fi

if [[ "$APPLY" == "true" ]]; then
  if [[ "$SERVICE" == "" ]]; then
    echo "When applying helm for specific service, you should set service name"
    echo -e "--help ('-h') \n\tDisplays help."
    exit
  fi
  case "$SERVICE" in
    "minio")
      set +e
      kubectl -n "$NAMESPACE" delete jobs minio-post-deploy-job
      set -e
      helm upgrade minio-release "$HELM_BASE_DIR/minio" --namespace "$NAMESPACE" --wait
      ;;
    "postgres")
      helm upgrade postgres-release "$HELM_BASE_DIR/postgres" --namespace "$NAMESPACE" --wait
      ;;
    "airflow")
      helm upgrade airflow apache-airflow/airflow --namespace "$NAMESPACE" --values "$HELM_BASE_DIR/airflow/values.yaml" --wait
      ;;
    "spark")
      helm upgrade spark spark-operator/spark-operator --namespace "$NAMESPACE" --values "$HELM_BASE_DIR/spark/values.yaml" --wait
      ;;
    "strimzi")
      helm upgrade strimzi-kafka-operator oci://quay.io/strimzi-helm/strimzi-kafka-operator \
        --namespace "$NAMESPACE" \
        --values "$HELM_BASE_DIR/strimzi-kafka/operator-values.yaml" \
        --wait
      ;;
    "kafka")
      helm upgrade --install kafka-cluster-crd "$HELM_BASE_DIR/kafka-cluster-crd" -n "$NAMESPACE" --wait
      kubectl wait kafka/kafka --for=condition=Ready --timeout=300s -n "$NAMESPACE"
      ;;
    "kafka-connect")
      kubectl wait kafka/kafka --for=condition=Ready --timeout=300s -n "$NAMESPACE"
      helm upgrade --install kafka-connect-crd "$HELM_BASE_DIR/kafka-connect-crd" -n "$NAMESPACE" --wait
      ;;
    "kafka-ui")
      kubectl wait kafka/kafka --for=condition=Ready --timeout=300s -n "$NAMESPACE"
      helm upgrade kafbat-ui kafbat-ui/kafka-ui -f "$HELM_BASE_DIR/kafbat-ui/values.yaml" -n "$NAMESPACE" --wait
      ;;
    "fake")
      helm upgrade "fake-release" "$HELM_BASE_DIR/fake" --namespace "$NAMESPACE" --values "$HELM_BASE_DIR/fake/values.yaml" --wait
      ;;
    *)
      echo "Service should be one of the following names: minio, postgres, airflow, spark"
      echo -e "--help ('-h') \n\tDisplays help."
      exit
      ;;
  esac
else
  if [[ "$TAG" == "" ]]; then
    echo "You should set --tag (or -t), since it's mandatory argument when you build image or during initial setup. We build the helm values based on tag. Use 'my_helm.sh -h' to see the help "
    exit
  fi
  source ./scripts/create_postgres.sh "$NAMESPACE"
  source ./scripts/create_minio.sh "$NAMESPACE"
  source ./scripts/create_spark.sh "$NAMESPACE"
  source ./scripts/create_airflow.sh "$NAMESPACE" "$TAG"
  source ./scripts/create_kafka.sh "$NAMESPACE" "$TAG"

  if [ -z "$KAFKA_BOOTSTRAP" ]; then
    exit
  fi
  echo "!!!!!!!!! $KAFKA_BOOTSTRAP"
  # NEW (portable and safe)
  FAKE_ENVS=$(cat <<EOF
env:
  PORT: 8090
  KAFKA_BROKERS: $KAFKA_BOOTSTRAP
  KAFKA_TOPIC: source-topic
EOF
  )
  echo "!!!!!!!! starting"
  source ./scripts/create_deployment.sh "-n=$NAMESPACE" "-s=fake" "-i=py-spark-fake" "-t=$TAG" "-p=8090" "-pf=8090" "$FAKE_ENVS"
fi

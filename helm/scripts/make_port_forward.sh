#!/bin/bash

NAMESPACE=$1
PORT_SERVICE=$2
PORT_FROM_FORWARD=$3
PORT_TO_FORWARD=$4
LABEL=$5

echo -e "-------------------------"
echo -e "-------PORT FORWARD------"
echo -e "--- $PORT_SERVICE ---"
echo -e "-------------------------"

set +e
PORT_BUSY=$(lsof -nP -i ":$PORT_TO_FORWARD" -a -c kubectl|awk '{print $2}'|sed -n '2p' || true)
if [ -n "$PORT_BUSY" ]; then
  echo "killing $PORT_BUSY"
  kill "$PORT_BUSY"
fi
export POD_NAME=$(kubectl get pods --namespace "$NAMESPACE" -l "$LABEL" -o jsonpath="{.items[0].metadata.name}" |  head -n 1)
kubectl -n "$NAMESPACE" port-forward "$POD_NAME" "$PORT_TO_FORWARD:$PORT_FROM_FORWARD" >/dev/null 2>&1 &
PID=$!
echo "$PORT_SERVICE on pod $POD_NAME port-forward $PORT_TO_FORWARD:$PORT_FROM_FORWARD with PID is $PID"
set -e
__Storing some commands__

Deleting py-spark images 
```shell
docker images|grep py-spark| awk '{print $3}'|awk '{printf "%s ", $0}'|xargs docker rmi -f
docker images|grep none| awk '{print $3}'|awk '{printf "%s ", $0}'|xargs docker rmi -f
```

Creating manually the iceberg-kafka-connect connector:
```shell
curl -sS -X POST http://localhost:8083/connectors/ \       
  -H 'Content-Type: application/json' \
  -d @connector.json
```

Finding existed opened ports to avoid conflicts during port-forward:
__Note:__ replace `8090` with desired port
```shell
lsof -nP -i ":8090" -a -c kubectl|awk '{print $2}'|sed -n '2p' || true
```
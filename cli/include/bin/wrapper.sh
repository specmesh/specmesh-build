#!/bin/bash

function provision() {
  echo "Provision..."
  exec java \
   -Xms64m -Xmx64m \
   -Dlog4j.configurationFile=/log/log4j2.xml \
   -cp "/opt/specmesh/service/lib/*" \
   io.specmesh.cli.Provision "$@"
}

function consumption() {
  echo "Consumption..."
  exec java \
   -Xms64m -Xmx64m \
   -Dlog4j.configurationFile=/log/log4j2.xml \
   -cp "/opt/specmesh/service/lib/*" \
   io.specmesh.cli.Consumption "$@"
}

function storage() {
  echo "Storage..."
  exec java \
   -Xms64m -Xmx64m \
   -Dlog4j.configurationFile=/log/log4j2.xml \
   -cp "/opt/specmesh/service/lib/*" \
   io.specmesh.cli.Storage "$@"
}
function export() {
  echo "Export..."
  exec java \
   -Xms64m -Xmx64m \
   -Dlog4j.configurationFile=/log/log4j2.xml \
   -cp "/opt/specmesh/service/lib/*" \
   io.specmesh.cli.Export "$@"

}


function usage() {
  echo "Usage "
  echo " Commands         [provision, consumption, storage, export]"
  echo " Common args      --bootstrap-server|-bs, --username,-u, --secret,-p"
  echo " Schema Reg args  --srUrl,-sr, --srApiKey,-srKey, --srApiSecret,-srSecret, --schemaPath,-schemaPath "
  echo " Other args       --spec,-spec, --appId,-appId "
  echo " Example:"
  echo "   docker run ghcr.io/specmesh/specmesh-build-cli provision -bs localhost:9092 -u admin -s admin-secret -sr https://localhost:1234 -srKey someKey -srSecret secret -schemaPath ./path "
 exit 1
}


if [ $# -le 4 ]; then
  usage
fi

case $1 in
  provision)
    shift
    provision "$@"
    ;;
  consumption)
    shift
    consumption "$@"
    ;;
  storage)
      shift
      storage "$@"
      ;;
  export)
      shift
      export "$@"
      ;;
  *)
    usage
    ;;
esac

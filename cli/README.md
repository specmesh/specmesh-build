# Kafka SpecMesh CLI

Provision, export and capture production & consumption metrics for a data product (app.yml)


## Simple Start

> % docker run --rm --network confluent -v "$(pwd)/resources:/app" ghcr.io/specmesh/specmesh-build-cli  provision -bs kafka:9092  -sr http://schema-registry:8081 -spec /app/simple_schema_demo-api.yaml -schemaPath /app




## Run a local kafka environment to manually test against (no security)

**Docker network**
> docker network create confluent


**ZooKeeper**
> docker run --name zookeeper -p 2181:2181 --network confluent -d zookeeper:latest
 
**Kafka**
> docker run --name kafka -p 9092:9092 --network confluent -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -d confluentinc/cp-kafka:latest
 
**Schema Registry**
>docker run --name schema-registry -p 8081:8081 --network confluent -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=kafka:9092 -e SCHEMA_REGISTRY_HOST_NAME=localhost -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 -d confluentinc/cp-schema-registry:latest



## Confirm the containers running in the docker-network called 'confluent'
> docker network inspect confluent


## Test that it works...using kafka producer-consumer

Create a topic
> docker exec -it kafka /bin/bash -c "/usr/bin/kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic test"

List topics
> docker exec -it kafka /bin/bash -c "/usr/bin/kafka-topics --list --bootstrap-server kafka:9092"



*Notice that the --bootstrap-server parameter now points to kafka:9092 instead of localhost:9092, as the Kafka container is now referred to by its container name within the confluent network.* 

Produce messages
> docker exec -it kafka /bin/bash -c "/usr/bin/kafka-console-producer --broker-list kafka:9092 --topic test"
> 
OR (own container rather than the 'kafka' container)
>  % docker run --name test-listing --network confluent -it  confluentinc/cp-kafka:latest  /bin/bash -c "/usr/bin/kafka-topics --list --bootstrap-server kafka:9092"

Consume messages
> docker exec -it kafka /bin/bash -c "/usr/bin/kafka-console-consumer --bootstrap-server kafka:9092 --topic test --from-beginning"






### **Cleanup time**

Stop the containers
>docker stop schema-registry\
>docker stop kafka\
>docker stop zookeeper
 
Remove the network
>docker network rm confluent


Remove containers
>docker rm schema-registry\
>docker rm kafka\
>docker rm zookeeper













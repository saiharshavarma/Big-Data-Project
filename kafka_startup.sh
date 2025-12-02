#!/bin/bash
set -euxo pipefail

# Install Docker
apt-get update -y
apt-get install -y docker.io

systemctl enable --now docker

# Create Docker network
docker network create kafka-net || true

# Run Zookeeper container
if [ ! "$(docker ps -q -f name=zookeeper)" ]; then
  if [ "$(docker ps -aq -f name=zookeeper)" ]; then
    docker rm -f zookeeper || true
  fi

  docker run -d \
    --name zookeeper \
    --network kafka-net \
    -p 2181:2181 \
    -e ZOOKEEPER_CLIENT_PORT=2181 \
    confluentinc/cp-zookeeper:latest
fi

# Run Kafka broker container
if [ ! "$(docker ps -q -f name=kafka)" ]; then
  if [ "$(docker ps -aq -f name=kafka)" ]; then
    docker rm -f kafka || true
  fi

  HOSTNAME="$(hostname)"

  docker run -d \
    --name kafka \
    --network kafka-net \
    -p 9092:9092 \
    -e KAFKA_BROKER_ID=1 \
    -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
    -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$HOSTNAME:9092 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    confluentinc/cp-kafka:latest
fi

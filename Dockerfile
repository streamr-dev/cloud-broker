# Ideas from:
# https://hackernoon.com/crafting-perfect-java-docker-build-flow-740f71638d63

# Use official OpenJDK 8 runtime as base image
FROM openjdk:8-jre-alpine

# Copy built "fat" JAR to container
COPY build/libs/broker-*-all.jar broker.war

# Default environment variables
ENV KAFKA_PORT 9092
ENV KAFKA_GROUP data-dev
ENV KAFKA_TOPIC data-dev
ENV REDIS_PASSWORD ""
ENV CASSANDRA_KEYSPACE streamr_dev

# Run broker when container launches
CMD java \
    -Dkafka.server=kafka:${KAFKA_PORT} \
    -Dkafka.group=${KAFKA_GROUP} \
    -Dkafka.topic=${KAFKA_TOPIC} \
    -Dredis.host=redis \
    -Dredis.password=${REDIS_PASSWORD} \
    -Dcassandra.host=cassandra \
    -Dcassandra.keyspace=${CASSANDRA_KEYSPACE} \
    -jar broker.war

#!/usr/bin/env bash
java -jar build/libs/broker-*-all.jar -Dkafka.server="dev.streamr:9093" -Dkafka.group="data-dev" -Dkafka.topic="data-dev" -Dredis.host="dev.streamr" -Dredis.password="AFuPxeVMwBKHV5Hm5SK3PkRZA" -Dcassandra.host="dev.streamr" -Dcassandra.keyspace="streamr_dev" -Dstats.interval="60"

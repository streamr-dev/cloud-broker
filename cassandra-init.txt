CREATE KEYSPACE streamr_dev WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE streamr_dev;

/* current */
CREATE TABLE stream_events (
    stream varchar,
    stream_partition int,
    kafka_partition int,
    kafka_offset bigint,
    previous_offset bigint,
    ts timestamp,
    payload blob,
    PRIMARY KEY ((stream, stream_partition), kafka_offset)
);

CREATE TABLE stream_timestamps (
    stream varchar,
    stream_partition int,
    kafka_offset bigint,
    ts timestamp,
    PRIMARY KEY ((stream, stream_partition), ts)
);

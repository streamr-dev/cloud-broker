# Broker
Consumes StreamrBinaryMessages from a Kafka topic and forwards them to Redis and
Cassandra.

## Performance test

The performance test is implemented in class `PerformanceTest.java`. Before
each test case we set up a fresh Kafka and Cassandra. The data is first pushed
to Kafka and then `Main.java#main` is invoked to start the broker process. The
data is pushed in advance to ensure that possible slowness of data generation 
process does not affect results.

| Total data | Queue size | Small payload (90%) | Large payload (10%) | Total messages | Write (kb/s)| msg / s |
|------------|------------|---------------------|---------------------|----------------|-------------|---------|
| 3941 MB    | 2000       | 100-400 bytes       | 2500-72000 bytes    | 1 000 000      | 40 000kb/s  | 10 300  |
| 7886 MB    | 2000       | 100-400 bytes       | 2500-72000 bytes    | 2 000 000      | 97 000kb/s  | 23 000  |
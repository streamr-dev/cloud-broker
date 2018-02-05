# Cloud Broker
An essential service of the Streamr cloud architecture responsible for message brokering. Listens to Apache Kafka for
new data and forwards it to Apache Cassandra (for long-term persistence) and Redis (for immediate consumption
by Streamr's engine-and-editor).

![Where Cloud Broker sits in Streamr cloud stack](high-level.png)

## Building

Project uses Gradle for build automation. We provide sensible default configurations for IntelliJ IDEA but project can be developed with other IDEs as well.

- Use Gradle task `test` to run tests.
- Use Gradle task `shadowJar` to build project into a Jar. 


## Misc
### Performance test

The performance test is implemented in class `PerformanceTest.java`. Before
each test case we set up a fresh Kafka and Cassandra. The data is first pushed
to Kafka and then `Main.java#main` is invoked to start the broker process. The
data is pushed in advance to ensure that possible slowness of data generation 
process does not affect results.

|Method                 | Total data | Queue size | Small payload (90%) | Large payload (10%) | Total messages | Write (kb/s)| msg / s |
|-----------------------|------------|------------|---------------------|---------------------|----------------|-------------|---------|
| CassandraRepoter      | 3941 MB    | 2000       | 100-400 bytes       | 2500-72000 bytes    | 1 000 000      | 40 000kb/s  | 10 300  |
| CassanraBatchReporter | 7886 MB    | 2000       | 100-400 bytes       | 2500-72000 bytes    | 2 000 000      | 97 000kb/s  | 23 000  |
| CassanraBatchReporter | 7890 MB    | 20000      | 100-400 bytes       | 2500-72000 bytes    | 2 000 000      | 97 000kb/s  | 25 000  |

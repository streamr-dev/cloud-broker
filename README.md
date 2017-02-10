# Broker
Consumes StreamrBinaryMessages from a Kafka topic and forwards them to Redis and Cassandra.

## Performance tests
How different ways of pushing 20.9 Gb of data performs.

### Cassandra

`CassandraReporter.java` with Queue size 2000 and 1 stream.

| #events  | size per event | total time (sec) | write (kB/s) | events/s |
|----------|:---------------|------------------|--------------|----------|
| 2500     |  8192 kB       | 95               | 219995       | 26.2     |
| 5000     |  4096 kB       | 101              | 205874       | 49.1     |
| 10000    |  2048 kB       | 97               | 215595       | 102.8    |
| 20000    |  1024 kB       | 94               | 222870       | 212.5    |
| 40000    |   512 kB       | 96               | 217231       | 414.3    |
| 160000   |   128 kB       | 108              | 193958       | 1479.5   |
| 1280000  |    16 kB       | 172              | 121587       | 7408.5   |
| 20480000 |     1 kB       | 818              |  26314       | 25013.5  |

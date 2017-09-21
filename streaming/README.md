## Streaming job
Spark Streaming job for parsing, denormalizing, and storing Kura protobuf metrics in Kudu, as well as analyzing metrics and generating alerts when machine is identified as being in a bad state.

##Usage

Build with:
`$ sbt clean assembly`

Submit with following args:
`<kafka-broker-list e.g. broker-1:9092,broker-2:9092> <kudu-master e.g. kudu-master-1:7051,kudu-master-2:7051 <mqtt-broker> <mqtt-user-name> <mqtt-password>`

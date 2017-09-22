Spark code for training ML models.

Execute in spark-shell or Cloudera Data Science Workbench (CDSW).

## Usage
Prereq: Training data should already be in HDFS at "/training-data".
### train-gateway-state-classifier-model.scala
Trains (and optionally publishes) the model to be served at the gateway.
1. This depends on [jpmml-spark-package](https://github.com/jpmml/jpmml-sparkml-package). First, fetch that.
`$ git clone https://github.com/jpmml/jpmml-sparkml-package.git`
2. Switch to Spark 1.6.x branch.
`$ cd jpmml-sparkml-package`
`$ git checkout spark-1.6.X`
3. Build with Maven.
`$ mvn clean package`
4. Switch back to batch/ directory.
`$ cd ..`
5. If you want to publish the trained model to Kafka and control whether the model is printed to console edit model-training.conf. To publish to Kafka, set "publish_pmml_to_kafka" to true and modify the dummy value for "kafka_broker_list" to point to your list of Kafka brokers.
`$ vi model-training.conf`
6. Execute in spark-shell.
`$ spark-shell -i train-gateway-state-classifier-model.scala --jars "jpmml-sparkml-package/target/jpmml-sparkml-package-1.0-SNAPSHOT.jar" --packages "org.apache.kafka:kafka-clients:0.10.0.0,com.typesafe:config:1.2.1"`

### train-core-state-classifier-model.scala
Execute in spark-shell to train and save the model to be served within Spark Streaming.
`$ spark-shell -i train-core-state-classifier-model.scala`


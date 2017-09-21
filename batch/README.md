Spark code for training ML models.

Execute in spark-shell or Cloudera Data Science Workbench (CDSW).

train-gateway-state-classifier.scala requires dependencies for [jpmml-spark-package](https://github.com/jpmml/jpmml-sparkml-package) and Kafka client libraries.

train-gateway-state-classifier.scala has parameter variables which control whether model PMML is printed, stored, and/or published to Kafka. Also variables controlling the path to store the model, and which Kafka broker and on which topic to publish the model.

By default both assume labeled training data has been placed in HDFS at "/training-data"

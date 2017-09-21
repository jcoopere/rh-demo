// Usage: spark-shell -i train-gateway-state-classifier.scala --jars jpmml-sparkml-package/target/jpmml-sparkml-package-1.0-SNAPSHOT.jar --packages "org.apache.kafka:kafka-clients:0.10.0.0"

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, RFormula}
import org.apache.spark.sql._

import java.io._
import java.util.Properties

case class LabeledTelemetry(label:Double, rpm:Double, voltage:Double, current:Double, temp:Double, db:Double, vibration:Double)

// Params
val sendKafka:Boolean = false
val persistPMML:Boolean = false
val printPMML:Boolean = true
val trainingDataDir = "/training-data/"
val modelSavePath = "/model/gateway-state-classifier-model"
val kafkaBrokerList = "ip-10-0-26-113.us-west-2.compute.internal:9092"
val kafkaTopic = "model"

val sqc = new SQLContext(sc)

val data = sc.textFile(trainingDataDir)

// Parse strings into DF
val points = data.map(line => {
	val split = line.split(" ")

	new LabeledTelemetry(split(0).toDouble, split(3).split(":")(1).toDouble, split(4).split(":")(1).toDouble, split(5).split(":")(1).toDouble, split(6).split(":")(1).toDouble, split(7).split(":")(1).toDouble, split(8).split(":")(1).toDouble)
}).toDF

// Formula
val formula = new RFormula().setFormula("label ~ .").setLabelCol("label").setFeaturesCol("features")

// Label indexer
val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(points)

// Random Forest Classifier
val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("features").setNumTrees(10)

// Convert labels
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

// Build Pipeline
val pipeline = new Pipeline().setStages(Array(formula, labelIndexer, rf, labelConverter))
val model = pipeline.fit(points)

if (model != null) {
	// Convert to PMML
	val pmmlBytes = org.jpmml.sparkml.ConverterUtil.toPMMLByteArray(points.schema, model)

	if (printPMML) {
		println(new String(pmmlBytes, "UTF-8"))
	}

	// Save to file
	if (persistPMML) {
		val file = new File(modelSavePath)
		val bw = new BufferedWriter(new FileWriter(file))
		bw.write(new String(pmmlBytes, "UTF-8"))
		bw.close()
	}

	// Send to Kafka
	if (sendKafka) {
		val props = new Properties
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerList)
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

		val producer = new KafkaProducer[String, String](props)

		val message = new ProducerRecord[String, String](kafkaTopic, null, new String(pmmlBytes, "UTF-8"))

		producer.send(message)
		producer.close()
	}
}

:quit
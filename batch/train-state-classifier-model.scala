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
val trainingDataDir = "/training-data/"
val kafkaBrokerList = ""
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

// Feature indexer
//val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(3).fit(points)

// Random Forest Classifier
val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("features").setNumTrees(10)

// Convert labels
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

// Build Pipeline
val pipeline = new Pipeline().setStages(Array(formula, labelIndexer, rf, labelConverter))
val model = pipeline.fit(points)

// Convert to PMML
val pmmlBytes = org.jpmml.sparkml.ConverterUtil.toPMMLByteArray(points.schema, model)

//println(new String(pmmlBytes, "UTF-8"))

// Send Kafka message
val props = new Properties
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerList)
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

val producer = new KafkaProducer[String, String](props)

val message = new ProducerRecord[String, String](kafkaTopic, null, new String(pmmlBytes, "UTF-8"))

producer.send(message)
producer.close()

// Save to file
/*val file = new File("/Users/jcooperellis/Documents/demos/iiot-demo/state-classifier.pmml")
val bw = new BufferedWriter(new FileWriter(file))
bw.write(new String(pmmlBytes, "UTF-8"))
bw.close()*/

:quit

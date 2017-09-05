import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql._

import java.io._

// Load training data in LIBSVM format.
//val data = MLUtils.loadLibSVMFile(sc, "/training-data")
case class LabeledTelemetry(label:Double, rpm:Double, voltage:Double, current:Double, temp:Double, db:Double, vibration:Double)

val sqc = new SQLContext(sc)

val data = sc.textFile("/Users/jcooperellis/Documents/demos/iiot-demo/redhat-demo/training-data/")

val points = data.map(line => {
	val split = line.split(" ")

	//new LabeledPoint(split(0).toDouble, Vectors.dense(split(3).split(":")(1).toDouble, split(4).split(":")(1).toDouble, split(5).split(":")(1).toDouble, split(6).split(":")(1).toDouble, split(7).split(":")(1).toDouble, split(8).split(":")(1).toDouble))
	new LabeledTelemetry(split(0).toDouble, split(3).split(":")(1).toDouble, split(4).split(":")(1).toDouble, split(5).split(":")(1).toDouble, split(6).split(":")(1).toDouble, split(7).split(":")(1).toDouble, split(8).split(":")(1).toDouble)
}).toDF

val formula = new RFormula().setFormula("label ~ .").setLabelCol("label").setFeaturesCol("features")

val rf = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").setNumTrees(10)

val pipeline = new Pipeline().setStages(Array(formula, rf))

val model = pipeline.fit(points)

val pmmlBytes = org.jpmml.sparkml.ConverterUtil.toPMMLByteArray(points.schema, model)

//println(new String(pmmlBytes, "UTF-8"))
val file = new File("/Users/jcooperellis/Documents/demos/iiot-demo/state-classifier.pmml")
val bw = new BufferedWriter(new FileWriter(file))
bw.write(new String(pmmlBytes, "UTF-8"))
bw.close()

:quit

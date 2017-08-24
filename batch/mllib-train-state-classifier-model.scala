import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils

// Load training data in LIBSVM format.
//val data = MLUtils.loadLibSVMFile(sc, "/training-data")

val data = sc.textFile("/training-data/")

val points = data.map(line => {
	val split = line.split(" ")

	new LabeledPoint(split(0).toDouble, Vectors.dense(split(3).split(":")(1).toDouble, split(4).split(":")(1).toDouble, split(5).split(":")(1).toDouble, split(6).split(":")(1).toDouble, split(7).split(":")(1).toDouble, split(8).split(":")(1).toDouble))
})


val splits = points.randomSplit(Array(0.7, 0.3))
val train = splits(0)
val test = splits(1)


// Run training algorithm to build the model
val numClasses = 3
val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 10
val featureSubsetStrategy = "auto"
val impurity = "gini"
val maxDepth = 10
val maxBins = 32

val model = RandomForest.trainClassifier(train, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)


val labelAndPreds = test.map(point => {
	val prediction = model.predict(point.features)
	(point.label, prediction)
})

val testErr = labelAndPreds.filter(r => { r._1 != r._2}).count.toDouble / test.count()
val testSucc = labelAndPreds.filter(r => { r._1 == r._2}).count.toDouble / test.count()

println("Test Error = " + testErr)
println("Test Success = " + testSucc)


model.save(sc, "/model/state-classifier-model")

:quit

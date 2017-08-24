package com.cloudera.demo.iiot

import java.util.HashMap

import scala.collection.JavaConverters._

import com.trueaccord.scalapb.spark._

import com.cloudera.demo.iiot.util.MaintenanceScheduler
import kafka.serializer._
import org.apache.log4j.{Level, Logger}
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{StreamingContext, Seconds}

import org.eclipse.kapua.service.device.call.message.kura.proto.kurapayload.KuraPayload

object IIoTDemoStreaming {

  case class Telemetry(motor_id:String, millis:Option[Long], metric:String, value:Option[Float])
  case class MotorPrediction(motorId:String, statePrediction:Double, ttfPrediction:Double)

  class MotorMetrics() {
    var speed = 0.0
    var voltage = 0.0
    var current = 0.0
    var temp = 0.0
    var noise = 0.0
    var vibration = 0.0
  }

  def main(args:Array[String]):Unit = {

    // Usage
    if (args.length == 0) {
      println("Args: <zk e.g. zookeeper-1:2181> <kafka-broker-list e.g. broker-1:9092,broker-2:9092> <kudu-master e.g. kudu-master-1:7051,kudu-master-2:7051")
      return
    }

    // Args
    val zk = args(0)
    val kafkaBrokerList = args(1)
    val kuduMasterList = args(2)

    // Hardcoded params
    val kafkaTopicIn = "ingest"
    val kafkaTopicOut = "event"
    val kuduTelemetryTable = "impala::iiot.telemetry"
    val kuduMaintenanceTable = "impala::iiot.maintenance"
    val stateModelDir = "/model/state-classifier-model"
    //val ttfModelDir = "/iiot-demo/model/ttf-regression-model"

    // Configure app
    val sparkConf = new SparkConf().setAppName("IIoTDemoStreaming")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(3))
    val sqc = new SQLContext(sc)
    val kc = new KuduContext(kuduMasterList)

    val maintenanceScheduler = new MaintenanceScheduler(kafkaBrokerList, kafkaTopicOut)

    import sqc.implicits._

    // Load models
    val stateModel = RandomForestModel.load(ssc.sparkContext, stateModelDir)
    //val ttfModel = LinearRegressionModel.load(ssc.sparkContext, ttfModelDir)

    // Consume messages from Kafka
    val kafkaConf = Map[String,String](
      "metadata.broker.list" -> kafkaBrokerList,
      "group.id" -> "IIoTDemoStreamingApp"
    )
    val kafkaDStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaConf, Set(kafkaTopicIn))
    kafkaDStream.print()

    // Parse raw messages values into protobuf objects
    val kurapayloadDStream = kafkaDStream.map(message => {
      val key = message._1
      val value = KuraPayload.parseFrom(message._2);

      (key, value)
    })

    // Convert (id, KuraPayload) tuple DStream into Telemetry DStream
    val telemetryDStream = kurapayloadDStream.flatMap(message => {
      val motor_id = message._1
      val millis = message._2.timestamp
      val metricsList = message._2.metric

      var telemetryArray = new Array[Telemetry](metricsList.length)

      var i = 0
      for (metric <- metricsList) {
        val metricValue = {
          if (metric.`type`.isDouble) metric.doubleValue.map(doubleVal => doubleVal.toFloat)
          else metric.floatValue
        }
        telemetryArray(i) = new Telemetry(motor_id, millis, metric.name, metricValue)
        i += 1
      }

      telemetryArray
    })

    // Convert each Telemetry RDD in the Telemetry DStream into a DataFrame and insert to Kudu
    telemetryDStream.foreachRDD(rdd => {
      val telemetryDF = rdd.toDF()

      kc.insertRows(telemetryDF, kuduTelemetryTable)
    })






    // ANALYTICS
    // For demo simplicity, only consider the highest timestamped payload per key in this microbatch.
    val kurapayloadMostRecentDStream = kurapayloadDStream.reduceByKey((payloadA:KuraPayload, payloadB:KuraPayload) => {
      if (payloadA.getTimestamp > payloadB.getTimestamp) payloadA
      else payloadB
    })

    val predictionDStream = kurapayloadMostRecentDStream.map(message => {
      val key = message._1
      val metrics = message._2.metric

      val metricsObj = new MotorMetrics()

      metrics.foreach(metric => {
        metric.name match {
          case "speed" => { metricsObj.speed = metric.getDoubleValue }
          case "voltage" => { metricsObj.voltage = metric.getDoubleValue }
          case "current" => { metricsObj.current = metric.getDoubleValue }
          case "temp" => { metricsObj.temp = metric.getDoubleValue }
          case "noise" => { metricsObj.noise = metric.getDoubleValue }
          case "vibration" => { metricsObj.vibration = metric.getDoubleValue }
        }
      })

      val vector = Vectors.dense(metricsObj.speed, metricsObj.voltage, metricsObj.current, metricsObj.temp, metricsObj.noise, metricsObj.vibration)

      val statePrediction = stateModel.predict(vector)
      //val ttfPrediction = ttfModel.predict(vector)
      val ttfPrediction = 14400000.0 // hardcoded to schedule maintenance 4 hours out

      (key, new MotorPrediction(key, statePrediction, ttfPrediction))
    })

    // Handle predictions.
/*    predictionDStream.foreachRDD(rdd => {
      rdd.foreach(value => {
        maintenanceScheduler.evaluate(value._1, value._2, value._3)
      })
    })
*/


    // Handle predictions.
    val maintenanceDStream = predictionDStream.updateStateByKey[MaintenanceScheduler]((predictions:Seq[MotorPrediction], maintenanceScheduler:Option[MaintenanceScheduler]) => {
      val scheduler = {
        if (maintenanceScheduler.isEmpty) new MaintenanceScheduler(kafkaBrokerList, kafkaTopicOut)
        else maintenanceScheduler.get
      }

      predictions.foreach(prediction => {
        scheduler.evaluate(prediction.motorId, prediction.statePrediction, prediction.ttfPrediction)
      })

      Some(scheduler)
    })








/*

    // Convert Protobufs into MotorMetrics objects
    val motormetricsDStream = kurapayloadDStream.map(message => {
      val key = message._1
      val speed = message._2.metric.withName("speed").getDoubleValue()
      val voltage = message._2.metric.withName("voltage").getDoubleValue()
      val current = message._2.metric.withName("current").getDoubleValue()
      val temp = message._2.metric.withName("temp").getDoubleValue()
      val noise = message._2.metric.withName("noise").getDoubleValue()
      val vibration = message._2.metric.withName("vibration").getDoubleValue()

      val value = new MotorMetrics(speed, voltage, current, temp, noise, vibration)

      (key, value)
    })

    // Reduce to a single MotorMetrics object that is the average of all in this microbatch
    val averagedMotorMetricsDStream = motormetricsDStream.reduceByKey((mm1, mm2) => {
      val averageSpeed = ((mm1.speed + mm2.speed) / 2)
      val averageVoltage = ((mm1.speed + mm2.speed) / 2)
      val averageCurrent = ((mm1.speed + mm2.speed) / 2)
      val averageTemp = ((mm1.speed + mm2.speed) / 2)
      val averageNoise = ((mm1.speed + mm2.speed) / 2)
      val averageVibration = ((mm1.speed + mm2.speed) / 2)

      new MotorMetrics(averageSpeed, averageVoltage, averageCurrent, averageTemp, averageNoise, averageVibration)
    })

    val averagedVectorsDStream =

    // Vectorize telemetry data, fit to time-to-failure regression model, and schedule maintenance.
    kurapayloadDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        // TODO
        partition.foreach(reading => {
          // TODO
        })
      })
    })
*/



/*
    // Parse messages into vectors, fit to model, and append classification.
    val readings = messages.map(message => {
      // Extract JSON from message.
      val json = parse(message._2)

      // Parse JSON.
      val pkgId = compact(render(json \ "pkgId"))
      val timestamp = compact(render(json \ "timestamp")).toInt
      val temperature = compact(render(json \ "temperature")).toFloat
      val humidity = compact(render(json \ "humidity")).toFloat
      val lat = compact(render(json \ "position" \ "lat")).toFloat
      val lng = compact(render(json \ "position" \ "lng")).toFloat
      val progress = compact(render(json \ "position" \ "progress")).toFloat
      val displacement = compact(render(json \ "displacement")).toFloat

      // Create a feature vector.
      val vector = Vectors.dense(temperature, humidity, displacement)

      // Make a prediction.
      val classification = model.predict(vector)

      // Interpret the result.
      val isPredictedDamaged = {
        if (classification == 1) true;
        else false;
      }

      // Return new JSON String.
      val json_out = 
        ("pkgId" -> pkgId) ~
        ("isPredictedDamaged" -> isPredictedDamaged)

      compact(render(json_out))
    })


*/

    ssc.checkpoint("/tmp/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
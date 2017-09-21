package com.cloudera.demo.iiot

import java.io.ByteArrayInputStream
import java.util.HashMap
import java.util.zip.GZIPInputStream

import scala.collection.JavaConverters._
import scala.util.Try

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
      println("Args: <zk e.g. zookeeper-1:2181> <kafka-broker-list e.g. broker-1:9092,broker-2:9092> <kudu-master e.g. kudu-master-1:7051,kudu-master-2:7051 <mqtt-broker> <mqtt-user-name> <mqtt-password>")
      return
    }

    // Args
    val zk = args(0)
    val kafkaBrokerList = args(1)
    val kuduMasterList = args(2)
    val mqttBroker = args(3)
    val mqttUserName = args(4)
    val mqttPassword = args(5)

    // Hardcoded params
    val kafkaTopicIn = "ingest"
    val kuduTelemetryTable = "impala::iiot.telemetry"
    val stateModelDir = "/model/state-classifier-model"

    // Configure app
    val sparkConf = new SparkConf().setAppName("IIoTDemoStreaming")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(3))
    val sqc = new SQLContext(sc)
    val kc = new KuduContext(kuduMasterList)

    import sqc.implicits._

    // Load models
    val stateModel = RandomForestModel.load(ssc.sparkContext, stateModelDir)

    // Consume messages from Kafka
    val kafkaConf = Map[String,String](
      "metadata.broker.list" -> kafkaBrokerList,
      "group.id" -> "IIoTDemoStreamingApp"
    )
    val kafkaDStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaConf, Set(kafkaTopicIn))

    // Parse raw messages values into protobuf objects
    val kurapayloadDStream = kafkaDStream.map(message => {
      val key = message._1

      val value:KuraPayload = {
        val tryUnzipAndParse = Try { KuraPayload.parseFrom(new GZIPInputStream(new ByteArrayInputStream(message._2))) }
        if (tryUnzipAndParse.isSuccess) {
          tryUnzipAndParse.get
        }
        else {
          KuraPayload.parseFrom(message._2)
        }
      }

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
          case other => {} // do nothing
        }
      })

      val vector = Vectors.dense(metricsObj.speed, metricsObj.voltage, metricsObj.current, metricsObj.temp, metricsObj.noise, metricsObj.vibration)

      val statePrediction = stateModel.predict(vector)

      // ttf currently hardcoded
      val ttfPrediction = statePrediction match {
        case 0.0 => 2592000000.0 // 30 days
        case 1.0 => 14400000.0 // 4 hours
        case 2.0 => 0 // now
      }

      (key, new MotorPrediction(key, statePrediction, ttfPrediction))
    })

    // Handle predictions.
    val maintenanceDStream = predictionDStream.updateStateByKey[MaintenanceScheduler]((predictions:Seq[MotorPrediction], maintenanceScheduler:Option[MaintenanceScheduler]) => {
      val scheduler = {
        if (maintenanceScheduler.isEmpty) new MaintenanceScheduler(mqttBroker, mqttUserName, mqttPassword)
        else maintenanceScheduler.get
      }

      predictions.foreach(prediction => {
        scheduler.evaluate(prediction.motorId, prediction.statePrediction, prediction.ttfPrediction)
      })

      Some(scheduler)
    })

    // PRINT
    //maintenanceDStream.print()

    ssc.checkpoint("/tmp/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
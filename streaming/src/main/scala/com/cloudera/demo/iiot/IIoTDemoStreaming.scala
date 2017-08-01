package com.cloudera.demo.cargo

import java.util.HashMap

import scala.collection.JavaConverters._

import com.trueaccord.scalapb.spark._

import kafka.serializer._
import org.apache.log4j.{Level, Logger}
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
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
    val kuduTelemetryTable = "impala::iiot.telemetry"
    //val modelDir = "/model/CargoRFModel"

    // Configure app
    val sparkConf = new SparkConf().setAppName("IIoTDemoStreaming")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(3))
    val sqc = new SQLContext(sc)
    val kc = new KuduContext(kuduMasterList)

    import sqc.implicits._

    // Load model
    //val model = RandomForestModel.load(streamingContext.sparkContext, modelDir)

    // Consume messages from Kafka
    val kafkaConf = Map[String,String](
      "metadata.broker.list" -> kafkaBrokerList,
      "group.id" -> "IIoTDemoStreamingApp"
    )
    val kafkaDStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaConf, Set(kafkaTopicIn))

    // Parse raw messages values into protobuf objects
    val kurapayloadDStream = kafkaDStream.map(message => {
      val key = message._1
      //val value = org.eclipse.kapua.service.device.call.message.kura.proto.kurapayload.KuraPayload.parseFrom(message._2);
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
        telemetryArray(i) = new Telemetry(motor_id, millis, metric.name, metric.floatValue)
        i += 1
      }

      telemetryArray
    })

    // Convert each Telemetry RDD in the Telemetry DStream into a DataFrame and insert to Kudu
    telemetryDStream.foreachRDD( rdd => {
      val telemetryDF = rdd.toDF()

      kc.insertRows(telemetryDF, kuduTelemetryTable)
    })

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

    // Write results to kafka.
    readings.foreachRDD(rdd => {
      rdd.foreachPartition ( partition => {
        val props = new HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerList)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)

        partition.foreach(reading => {
          val message = new ProducerRecord[String, String](kafkaTopicOut, null, reading)
          producer.send(message)
        })
        producer.close()
      })
    })
*/

    ssc.checkpoint("/tmp/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
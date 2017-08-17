package com.cloudera.demo.iiot.util

import java.lang.System
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence

import scala.collection.mutable.HashSet

/**
 * A utility simulating integration with an enterprise maintenance scheduling system.
 *
 * In practice, this would evaluate the current predicted maintenance needs of a
 * particular machine and coordinate with other systems.
 */
case class MaintenanceEvent(eventId:String, motorId:String, createdTime:Long, startTime:Long, endTime:Long, technician:String, reason:String, priority:String, partId:String, instructions:String) {
	def toJsonString():String = {
		var json = s"""{\"eventId\":\"$eventId\",\"motorId\":\"$motorId\",\"createdTime\":$createdTime,\"startTime\":$startTime,\"endTime\":endTime,\"technician\":\"$technician\",\"reason\":\"$reason\",\"priority\":\"$priority\",\"partId\":\"$partId\",\"instructions\":\"$instructions\"}"""" // " So the triple quotes don't confused the IDE. Scala is weird...		
		
		return json
	}
}

class MaintenanceScheduler(mqttBroker:String, mqttTopic:String) extends Serializable {
	var maintenanceScheduled = false

	// Simulate evaluating the machine state against schedule and other considerations.
	// Here, all we do is see if the motor is in a bad state and schedule an event if we haven't already.
	// If we schedule new maintenance, send a message containing information about it over the "event" Kafka topic.
	// If the motor is in a good state, reset.
	def evaluate(motorId:String, state:String, ttf:Double) = {
		state match {
			case "BAD POWER SUPPLY" => { 
				if (!maintenanceScheduled) {
					// Currently this is all hardcoded to enable only the specific demo scenario.
					val eventId = "PSP10001"
					val createdTime = System.currentTimeMillis()
					val startTime = createdTime + ttf.toLong
					val endTime = startTime + (1000 * 60 * 60) // one hour maintenance window
					val technician = "Webster Izlayme"
					val reason = s"PREDICTIVE MAINTENANCE: BAD POWER SUPPLY. PREDICTED CRITICAL MOTOR FAILURE IN $ttf MILLISECONDS."
					val priority = "CRITICAL"
					val partId = "MTCP-050-3BD18"
					val instructions = "CHECK & REPAIR POWER SUPPLY. REPLACE INDUCTION MOTOR. REFER TO MANUAL FOR STEP-BY-STEP INSTRUCTIONS: https://cdn.automationdirect.com/static/manuals/ironhorsemanual/ironhorsemanual.html"

					val event = new MaintenanceEvent(eventId, motorId, createdTime, startTime, endTime, technician, reason, priority, partId, instructions)

					publishEventOverMqtt(event)
/*
					// Send Kafka message
					val props = new Properties
			        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerList)
			        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
			        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

			        val producer = new KafkaProducer[String, String](props)

			        val message = new ProducerRecord[String, String](kafkaTopic, null, event.toJson())

			        producer.send(message)
			        producer.close()
*/
					maintenanceScheduled = true
				}
			}
			case _ => {
				if (maintenanceScheduled) {
					maintenanceScheduled = false
				}
			}
		}
	}

	private def publishEventOverMqtt(event:MaintenanceEvent) {
		var client:MqttClient = null

		try {
			val client = new MqttClient(mqttBroker, MqttClient.generateClientId, new MqttDefaultFilePersistence("/tmp"))

			client.connect()

			val msgTopic = client.getTopic(mqttTopic)
			val msg = new MqttMessage(event.toJsonString.getBytes("utf-8"))

			msgTopic.publish(msg)
		} catch {
			case e:MqttException => println(e)
		} finally {
			client.disconnect()
		}
	}
}
/*
object MaintenanceScheduler {
	implicit def publishEventOverMqtt(event:MaintenanceEvent) {
		try {
			val client = new MqttClient(mqttBroker, MqttClient.generateClientId, new MqttDefaultFilePersistence("/tmp"))

			client.connect()

			val msgTopic = client.getTopic(mqttTopic)
			val msg = new MqttMessage(event.toJsonString.getBytes("utf-8"))

			msgTopic.publish(msg)
		} catch {
			case e:MqttException => println(e)
		} finally {
			client.disconnect()
		}
	}
}

*/









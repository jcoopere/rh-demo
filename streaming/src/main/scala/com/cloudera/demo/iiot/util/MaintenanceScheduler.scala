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
case class MaintenanceEvent(eventId:String, description:String, timestamp:Long, mType:String, reason:String, startTime:Long, endTime:Long) {
	def toJsonString():String = {
		var json = s"""{\"id\":\"$eventId\",\"description\":\"$description\",\"timestamp\":$timestamp,\"type\":\"$mType\",\"details\":{\"reason\":\"$reason\",\"start\":$startTime,\"end\":$endTime}}"""" // " So the triple quotes don't confused the IDE. Scala is weird...		
		
		return json
	}
}

class MaintenanceScheduler(mqttBroker:String) extends Serializable {
	var maintenanceScheduled = false

	// Simulate evaluating the machine state against schedule and other considerations.
	// Here, all we do is see if the motor is in a bad state and schedule an event if we haven't already.
	// If we schedule new maintenance, send a message containing information about it over the "event" Kafka topic.
	// If the motor is in a good state, reset.
	def evaluate(motorId:String, state:Double, ttf:Double) = {
		state match {
			case 1.0 => { 
				if (!maintenanceScheduled) {
					// Currently this is all hardcoded to enable only the specific demo scenario.
					val eventId = "D846E916-FA87-4ACE-97A6-D0C91C5116C6"
					val description = "Maintenance Required"
					val timestamp = System.currentTimeMillis()
					val mType = "maintenance"
					val reason = "Predictive Maintenance Alert: Machine predicted in state BAD_POWER_SUPPLY with impending failure."
					val startTime = timestamp + ttf.toLong
					val endTime = startTime + (1000 * 60 * 60) // one hour maintenance window

					val event = new MaintenanceEvent(eventId, description, timestamp, mType, reason, startTime, endTime)

					publishEventOverMqtt(motorId, event)
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

	private def publishEventOverMqtt(motorId:String, event:MaintenanceEvent) {
		var client:MqttClient = null

		try {
			val client = new MqttClient(mqttBroker, MqttClient.generateClientId, new MqttDefaultFilePersistence("/tmp"))

			client.connect()

			val msgTopic = client.getTopic(motorId + "/alerts")
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









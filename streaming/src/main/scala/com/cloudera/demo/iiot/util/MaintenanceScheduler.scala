package com.cloudera.demo.iiot.util

import java.lang.System
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j._

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
		var json = s"""{\"id\":\"$eventId\",\"description\":\"$description\",\"timestamp\":$timestamp,\"type\":\"$mType\",\"details\":{\"reason\":\"$reason\",\"start\":$startTime,\"end\":$endTime}}""" // " So the triple quotes don't confused the IDE. Scala is weird...		
		
		return json
	}
}

class MaintenanceScheduler(mqttBroker:String, mqttUserName:String, mqttPassword:String) extends Serializable {
	@transient lazy val log = org.apache.log4j.Logger.getLogger(getClass.getName)
	log.setLevel(Level.INFO)

	var failureScheduled = false
	var mostRecentMessage = ""
	var lastPredictedState = 0.0
	var sequentialStatePredictionCount = 0

	// Simulate evaluating the machine state against schedule and other considerations.
	// Here, all we do is see if the motor is in a bad state and schedule an event if we haven't already.
	// If we schedule new maintenance, send a message containing information about it over the alerts MQTT topic.
	// If the motor is in a good state, reset.
	def evaluate(motorId:String, state:Double, ttf:Double) = {
		state match {
			case 0.0 => {
				if (failureScheduled) {
					if (state == lastPredictedState) {
						if (sequentialStatePredictionCount > 2) {
							failureScheduled = false
						} else {
							sequentialStatePredictionCount += 1
						}
					} else {
						lastPredictedState = state
						sequentialStatePredictionCount = 1
					}
				}
			}
			case 1.0 => { 
				if (!failureScheduled) {
					if (state == lastPredictedState) {
						if (sequentialStatePredictionCount > 2) {
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

							failureScheduled = true
						} else {
							sequentialStatePredictionCount += 1
						}
					} else {
						lastPredictedState = state
						sequentialStatePredictionCount = 1
					}
				}
			}
			case 2.0 => { 
				if (!failureScheduled) {
					if (state == lastPredictedState) {
						if (sequentialStatePredictionCount > 2) {
							// Currently this is all hardcoded to enable only the specific demo scenario.
							val eventId = "D846E916-FA87-4ACE-97A6-D0C91C5116C6"
							val description = "Maintenance Required"
							val timestamp = System.currentTimeMillis()
							val mType = "maintenance"
							val reason = "Predictive Maintenance Alert: Machine predicted in state ROTOR_LOCK with immediate failure."
							val startTime = timestamp + ttf.toLong
							val endTime = startTime + (1000 * 60 * 60 * 2) // two hour maintenance window

							val event = new MaintenanceEvent(eventId, description, timestamp, mType, reason, startTime, endTime)

							publishEventOverMqtt(motorId, event)

							failureScheduled = true
						} else {
							sequentialStatePredictionCount += 1
						}
					} else {
						lastPredictedState = state
						sequentialStatePredictionCount = 1
					}
				}
			}
		}
	}

	private def publishEventOverMqtt(motorId:String, event:MaintenanceEvent) {
		log.info("Attempting to publish MQTT message...")
		var client:MqttClient = null

		try {
			log.info("Connecting to MQTT client at broker: " + mqttBroker)
			client = new MqttClient(mqttBroker, MqttClient.generateClientId, new MqttDefaultFilePersistence("/tmp"))
			val opts = new MqttConnectOptions()
			opts.setUserName(mqttUserName)
			opts.setPassword(mqttPassword.toCharArray)

			log.info("Connecting to MQTT client at broker: " + mqttBroker)
			client.connect(opts)
			log.info("Client connection successful!")

			val topic = motorId + "/alerts"
			val msgTopic = client.getTopic(topic)
			log.info("Attempting to send message: " + event.toJsonString + " on topic: " + topic)
			//val msg = new MqttMessage(event.toJsonString.getBytes("utf-8"))

			mostRecentMessage = "Topic: " + motorId + "/alerts Payload:" + event.toJsonString

			//msgTopic.publish(msg)
			msgTopic.publish(event.toJsonString.getBytes("utf-8"), 0, false)

			log.info("Message published!")
		} catch {
			case e:Exception => log.error(e)
		} finally {
			if (client != null) client.disconnect()
		}
	}

	override def toString:String = {
		s"MaintenanceScheduler($failureScheduled,$lastPredictedState,$sequentialStatePredictionCount,$mostRecentMessage)"
	}
}









package com.lawsofnatrue.logback

import java.util.Properties

import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import org.apache.kafka.clients.producer._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by fangzhongwei on 2016/10/18.
  */
class KafkaAppender extends AppenderBase[ILoggingEvent] {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  var encoder: PatternLayoutEncoder = null
  var producer: Producer[String, String] = null
  var producerProperties: String = ""
  var topic: String = ""

  private var connected: Boolean = false

  override def append(e: ILoggingEvent): Unit = {
    if (connected) {
      producer.send(new ProducerRecord(topic, encoder.getLayout.doLayout(e)))
    }
  }

  override def start(): Unit = {
    super.start()
    init
  }


  override def stop(): Unit = {
    super.stop()
    if (producer != null) {
      producer.close()
    }
  }

  def init: Unit = {
    checkConfig
    new Thread(new Runnable {
      override def run(): Unit = {
        initProducer
      }
    }).start()
  }

  def checkConfig: Unit = {
    if (encoder == null) {
      throw new Exception("encoder must be config")
    }
    if (producerProperties == null || producerProperties.trim.length == 0) {
      throw new Exception("producerProperties cannot be empty")
    }
    if (topic == null || topic.trim.length == 0) {
      throw new Exception("topic cannot be empty")
    }
  }

  def initProducer: Unit = {
    try {
      val props: Properties = parseProducerConfig
      producer = new KafkaProducer[String, String](props)
      producer.send(new ProducerRecord("server-startup", "member-server"), new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          if (e != null) {
            logger.error("connect kafka error", e)
            connected = false
            Thread.sleep(5000)
            init
          } else {
            logger.info("connect kafka success")
            connected = true
          }
        }
      })
    } catch {
      case e: Exception =>
        logger.error("connect kafka error", e)
    }
  }

  def parseProducerConfig: Properties = {
    val props: Properties = new Properties
    val strings: Array[String] = producerProperties.replaceAll("\\s", "").split(";") filter (_.length > 0)
    var configItem: Array[String] = Array()
    for (item <- strings) {
      configItem = item.split("=")
      if (configItem.length == 2) {
        props.put(configItem(0), configItem(1))
      }
    }
    props
  }

  def setProducerProperties(params: String): Unit = {
    logger.info("producer properties is :" + params)
    this.producerProperties = params
  }

  def setTopic(topic: String): Unit = {
    logger.info("topic is :" + topic)
    this.topic = topic
  }

  def setEncoder(encoder: PatternLayoutEncoder): Unit = {
    logger.info("encoder layout is :" + encoder.getLayout)
    this.encoder = encoder
  }
}
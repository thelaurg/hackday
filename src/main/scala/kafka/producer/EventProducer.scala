package kafka.producer

import kafka.producer.ProducerConfig
import java.util.Properties
import kafka.producer.Producer
import scala.util.Random
import kafka.producer.KeyedMessage


case class EventProducer(eventType: String, topic: String, brokerList: String) {

  def send = {
    val props =  new Properties()

    props.put("metadata.broker.list", brokerList)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")

    val config = new ProducerConfig(props)

    val producer = new Producer[String, String](config)

    (1 to 10000).foreach { _ =>
      val data = new KeyedMessage[String, String](topic, eventType, new Random().nextString(10))

      producer.send(data)
    }

    producer.close()
  }
}

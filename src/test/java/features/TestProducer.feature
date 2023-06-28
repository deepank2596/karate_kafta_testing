Feature: Kafka Producer and Consumer Demo

Background:
  * def KafkaProducer = Java.type('karate.kafka.KarateKafkaProducer')
  * def KafkaConsumer = Java.type('karate.kafka.KarateKafkaConsumer')
  * def topic = 'my-first'

Scenario: Write messages to "my-first" and read it back

  * def kp = new KafkaProducer()
  * def props = KafkaConsumer.getDefaultProperties()
  * def kc = new KafkaConsumer(topic,props)
  * def key = "message_key"
  * def value =
  """
  {
    person : {
          firstName : "Deepank",
          lastName : "Gupta"
        },
    location : "Japan"
  }
  """
  * def headers = { x-header-one : "header-one-value", x-header-two : "header-two-value" }
  * kp.send(topic, key, value,headers);

  # Read from the consumer
  * json out = kc.take()

  * kp.close()
  * kc.close()

  # Match
  * match out.key == "message_key"
  * match out.value.person.firstName == 'Deepank'
  * match out.headers contains { "x-header-one": "header-one-value" }
  * match out.headers contains { "x-header-two": "header-two-value" }
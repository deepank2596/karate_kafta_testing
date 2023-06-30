package karate.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.JsonPathException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.jayway.jsonpath.internal.Utils.isEmpty;
import static java.util.Objects.isNull;

import java.time.Duration;

public class KarateKafkaConsumerWithSeek {

  private static Logger logger = LoggerFactory.getLogger(KarateKafkaConsumerWithSeek.class.getName());
  private KafkaConsumer<Object, Object> kafka;
  private Pattern keyFilter; // Java regular expression
  private String valueFilter; // Json path expression

  private BlockingQueue<String> outputList = new LinkedBlockingQueue<>();

  public KarateKafkaConsumerWithSeek(String kafkaTopic, Map<String, String> consumerProperties) {
    this(kafkaTopic, consumerProperties, null, null);
  }

  public KarateKafkaConsumerWithSeek(String kafkaTopic) {
    this(kafkaTopic, null, null);
  }

  public KarateKafkaConsumerWithSeek( String kafkaTopic, String keyFilterExpression, String valueFilterExpression) {
    Properties cp = getDefaultProperties();
    setKeyValueFilters(keyFilterExpression, valueFilterExpression);
    create(kafkaTopic, cp);
  }

  public KarateKafkaConsumerWithSeek(
      String kafkaTopic,
      Map<String, String> consumerProperties,
      String keyFilterExpression,
      String valueFilterExpression) {

      setKeyValueFilters(keyFilterExpression, valueFilterExpression);
      Properties cp = new Properties();
      for (String key : consumerProperties.keySet()) {
        String value = consumerProperties.get(key);
        cp.setProperty(key, value);
      }
      create(kafkaTopic, cp);
  }

  // All constructors eventually call this ....
  private void create(String kafkaTopic, Properties cp) {

    // Create the consumer and subscribe to the topic
    kafka = new KafkaConsumer<Object, Object>(cp);
    //kafka.subscribe(Collections.singleton(kafkaTopic));
    // assign
    TopicPartition partitionToReadFrom = new TopicPartition(kafkaTopic, 0);
    long offsetToReadFrom = 7L;
    kafka.subscribe(Collections.singleton(kafkaTopic));

    // seek
    kafka.seekToEnd(Arrays.asList());
    
    logger.debug("Waiting for consumer to be ready..");
    logger.debug("consumer is ready");
  }

  /**
   * Sets the predicate to filter kafka records based on key or/and value
   *
   * @param keyFilterExpression Java regular expression pattern
   * @param valueFilterExpression <a href="https://github.com/json-path/JsonPath">JsonPath</a>
   *     expression
   */
  private void setKeyValueFilters(String keyFilterExpression, String valueFilterExpression) {
    if (!isEmpty(keyFilterExpression)) {
      this.keyFilter = Pattern.compile(keyFilterExpression);
    }
    if (!isEmpty(valueFilterExpression)) {
      this.valueFilter = valueFilterExpression;
    }
    
  }

  public static Properties getDefaultProperties() {
    // Consumer Configuration
    Properties cp = new Properties();
    cp.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

    cp.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    cp.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    cp.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "karate-kafka-default-consumer-group");
    return cp;
  }

  public synchronized void read_data_from_producer(int numberOfMessagesToRead){
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;
        try {
        final int giveUp = 100;   int noRecordsCount = 0;
        // poll for new data
        while(keepOnReading){
            ConsumerRecords<Object, Object> records =
                    kafka.poll(Duration.ofMillis(100));
            if (records.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                //else continue;
            }

            for (ConsumerRecord<Object, Object> record : records){
                numberOfMessagesReadSoFar += 1;
                System.out.println("Key: " + record.key() + ", Value: " + record.value()+", Partition: " + record.partition() + ", Offset:" + record.offset());
                // We want to return a String that can be interpreted by Karate as a JSON
                Object key = record.key();
                Object value = record.value();
                Headers recordHeaders = record.headers();

                String str = convertToJsonString(key, value, recordHeaders);

                if (!isNull(keyFilter) && !filterByKey(key)) {
                  continue;
                }
                if (!isNull(valueFilter) && !filterByValue(value)) {
                  continue;
                }
                logger.debug("Consuming record. key: " + key + ", value: " + value);
                outputList.put(str);
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepOnReading = false; // to exit the while loop
                    break; // to exit the for loop
                }
            }
        }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
  }



  private String convertToJsonString(Object key, Object value, Headers recordHeaders){
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String,String> map = new HashMap<>();
    for(Header h : recordHeaders) {
      String headerKey = h.key();
      String headerValue = new String(h.value());
      map.put(headerKey,headerValue);
    }

    if( map.size() == 0 ) {
      // There were no headers
      return  "{key: " + key + ", value: " + value + "}";
    }
    else {
      // headers are present ...
      String headers;
      try {
        headers = objectMapper.writeValueAsString(map);
      } catch (JsonProcessingException e) {
        headers =  "error";
        logger.error("Unable to parse header");
      }
      return "{key: " + key + ", value: " + value + ", headers: " + headers + "}";
    }
  }

  /**
   * @param value The kafka record value
   * @return
   */
  private boolean filterByValue(Object value) {
    try {
      return !isNull(value)
          && !JsonPath.parse(value.toString()).read(valueFilter, List.class).isEmpty();
    } catch (JsonPathException e) {
      logger.error("Exception while trying to filter value", e);
    }
    return false;
  }

  /**
   * Checks whether the given string matches the keyFilter regular expression
   *
   * @param key String to be checked for pattern matching
   * @return true if the key matches the keyFilter pattern. False otherwise
   */
  private boolean filterByKey(Object key) {
    return !isNull(key) && keyFilter.matcher(key.toString()).find();
  }

  /**
   * @return The next available kafka record in the Queue (head of the queue). If no record is
   *     available, then the call is blocked.
   * @throws InterruptedException - if interrupted while waiting
   */
  public synchronized String take() throws InterruptedException {
    read_data_from_producer(1);
    logger.debug("take() called");
    if(outputList.size()!=0) return outputList.take(); // wait if necessary for data to become available
    return "Not Data Found";
  }

  /**
   * @param n  The number of records to read
   * @return The next available kafka record in the Queue (head of the queue). If no record is
   *     available, then the call is blocked.
   * @throws InterruptedException - if interrupted while waiting
   */
  public synchronized String take(int n) throws InterruptedException {
    read_data_from_producer(n);
    logger.debug("take(n) called");
    if(outputList.size()==0) return "Not Data Found";
    List<String> list = new ArrayList<>();
    for(int i=0; i<n; i++){
      list.add(outputList.take()); // wait if necessary for data to become available
    }
    // We want to return a String that can be interpreted by Karate as a JSON
    String str = list.toString();
    return str;
  }

  /**
   * @param timeout  maximum time in milliseconds to wait for a record
   * @return The next available kafka record in the Queue (head of the queue). If no record is
   *     available for timeout milliseconds, then return null
   * @throws InterruptedException - if interrupted while waiting
   */
  public synchronized String poll(long timeout) throws InterruptedException {
    logger.debug("poll() called");
    return outputList.poll(timeout, TimeUnit.MILLISECONDS);
  }


  public static void main(String[] args) throws InterruptedException {
      KarateKafkaConsumerWithSeek kc = new KarateKafkaConsumerWithSeek("my-first");
      String out = kc.take();
      System.out.println(out);
  }

}

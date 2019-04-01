package com.dom.kafka.samples.kafkasamples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class ConsumerSample {

	private static final String TOPIC = "first-topic";
	private static final Logger logger = LoggerFactory.getLogger(ConsumerSample.class);

	private Map<String, Object> getProperties(String group) {
		Map<String, Object> properties = new HashMap<>();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		return properties;
	}

	private void printRecords(String consumerName, KafkaConsumer<String, String> consumer) {
		var consumer1Pool = consumer.poll(Duration.ofMillis(500));
		consumer1Pool.records(ConsumerSample.TOPIC).forEach(data -> {
			logger.info(String.format("%s - Topic: %s, Partition: %s, Offset: %s",
					consumerName, data.topic(), data.partition(), data.offset()));
		});
	}

	public static void main(String[] args) {

		var sample = new ConsumerSample();
		var consumer1 = new KafkaConsumer<String, String>(sample.getProperties("fist-group"));
		var consumer2 = new KafkaConsumer<String, String>(sample.getProperties("fist-group"));

		consumer1.subscribe(Collections.singleton(ConsumerSample.TOPIC));
		consumer2.subscribe(Collections.singleton(ConsumerSample.TOPIC));

		sample.printRecords("consumer1", consumer1);
		sample.printRecords("consumer2", consumer2);

		sample.waitMethod();
	}

	private synchronized void waitMethod() {
		while (true) {
			try {
				this.wait(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}

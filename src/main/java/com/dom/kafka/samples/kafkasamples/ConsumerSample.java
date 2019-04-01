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
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class ConsumerSample {

	private static final String TOPIC = "second-topic";
	private static final Logger logger = LoggerFactory.getLogger(ConsumerSample.class);

	private Map<String, Object> getProperties(String group, String offsetConfig) {
		Map<String, Object> properties = new HashMap<>();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig);
		return properties;
	}

	private void printRecords(String consumerName, KafkaConsumer<String, String> consumer) {
		var consumer1Pool = consumer.poll(Duration.ofMillis(500));
		consumer1Pool.records(ConsumerSample.TOPIC).forEach(data -> {
			logger.info(String.format("%s - Topic: %s, Partition: %s, Offset: %s",
					consumerName, data.topic(), data.partition(), data.offset()));
		});
	}

	public static void main(String[] args) throws ExecutionException, InterruptedException{

		/**
		 * Create consumers on the same group.
		 *
		 * The AUTO_OFFSET_RESET_CONFIG=earliest should read all records from all the partitions from the offset 0.
		 * The messages will also be ordered by the offset 0, 1, 2...n.
		 */
		var sample = new ConsumerSample();
		var consumer1 = new KafkaConsumer<String, String>(sample.getProperties("fist-group", "earliest"));
		var consumer2 = new KafkaConsumer<String, String>(sample.getProperties("fist-group", "earliest"));
		/**
		 * Subscribe on the same topic
		 */
		consumer1.subscribe(Collections.singleton(ConsumerSample.TOPIC));
		consumer2.subscribe(Collections.singleton(ConsumerSample.TOPIC));

		/**
		 * Create producer.
		 */
		ProducerSample producerSample = new ProducerSample();
		var kafkaProducer = producerSample.getProducer("consumer-app-sample");


		/**
		 * Write records on the same topic and different key.
		 *
		 * The producer should balance the messages on 3 different partitions because the keys are all different.
		 */
		for (int i = 0; i < 20 ; i++) {
			var record = new ProducerRecord<>(ConsumerSample.TOPIC, "key_" + i, "message");
			kafkaProducer.send(record, (RecordMetadata metadata, Exception exception) -> {
				if (exception == null) {
					logger.info(String.format("Sent to: Topic: %s, offset: %s, partition: %s", metadata.topic(), metadata.offset(), metadata.partition()));
				}
			}).get();
		}
		kafkaProducer.flush();
		kafkaProducer.close();

		/**
		 * Print all the records for each consumer.
		 */
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

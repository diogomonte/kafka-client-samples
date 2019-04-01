package com.dom.kafka.samples.kafkasamples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class ProducerSample {

	public static void main(String[] args) {
		final Logger logger = LoggerFactory.getLogger(ProducerSample.class);

		Map<String, Object> properties = new HashMap<>();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "consumer-sample");
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		var producer = new KafkaProducer<String, String>(properties);
		var producerRecord = new ProducerRecord<String, String>("first-topic", "message");
		producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
			if (exception == null) {
				logger.info("Message has been sent.");
				logger.info(String.format("Topic: %s, offset: %s, partition: %s",
						metadata.topic(), metadata.offset(), metadata.partition()));
			} else {
				logger.error("Error sending message!");
			}
		});
		producer.flush();
		producer.close();
	}
}

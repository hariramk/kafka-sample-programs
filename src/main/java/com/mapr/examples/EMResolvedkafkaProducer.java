package com.mapr.examples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.io.Resources;

public class EMResolvedkafkaProducer {
	public void sendResolved(String topic, String message) throws IOException {

		// set up the producer
		KafkaProducer<String, String> producer;
		try (InputStream props = Resources.getResource("producer.props").openStream()) {
			Properties properties = new Properties();
			properties.load(props);
			producer = new KafkaProducer<>(properties);
		}

		try {
			// send lots of messages
			producer.send(new ProducerRecord<String, String>(topic, message));
			producer.flush();
		} catch (Throwable throwable) {
			System.out.printf("%s", throwable.getStackTrace());
		} finally {
			producer.close();
		}

	}
}

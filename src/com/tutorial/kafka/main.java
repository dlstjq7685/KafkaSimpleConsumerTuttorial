package com.tutorial.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;;

public class main {

	private final static String TOPIC = "quickstart-events";
	private final static String BOOTSTRAP_SERVERS =
			"localhost:9092,localhost:9093,localhost:9094";

	private final static Logger logger = LoggerFactory.getLogger(main.class);

	public static void main(String[] args) throws Exception {

		logger.info("consumer run");		
		
		runConsumer();

	}

	private static Consumer<String, String> createConsumer() {

		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG,
				"KafkaSimpleConsumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());

		// Create the consumer using props.
		final Consumer<String, String> consumer =
				new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());

		List<String> topics = new ArrayList<>();

		topics.add(TOPIC);
		topics.add("next-event");

		// Subscribe to the topic.
		consumer.subscribe(topics);

		return consumer;
	}

	static void runConsumer() throws InterruptedException {
		final Consumer<String, String> consumer = createConsumer();

		final int giveUp = 100;   
		int noRecordsCount = 0;

		while (true) {
			final ConsumerRecords<String, String> consumerRecords =
					consumer.poll(Duration.ofMillis(1000));

			if (consumerRecords.count() == 0) {
				noRecordsCount++;
				
				if (noRecordsCount > giveUp) break;
				else continue;
			}

			consumerRecords.forEach(record -> {
				System.out.printf("Consumer Record:(%s, %s, %d, %s, %s)\n",
						record.key(), record.value(),
						record.partition(), record.offset(), record.topic());
			});

			consumer.commitAsync();
		}

		consumer.close();
		
		logger.info("DONE");
	}

}

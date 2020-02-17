package com.velatech.cardverifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CardVerifierApplication {

    public static void main(String[] args) {
        SpringApplication.run(CardVerifierApplication.class, args);
        
    	final String TOPIC = "com.ng.vela.even.card_verified";
    	
        Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("group.id", "vela");

		KafkaConsumer<String, String> myConsumer = new KafkaConsumer<String, String>(props);

		myConsumer.subscribe(Arrays.asList(TOPIC));

		try {
			while (true) {
				ConsumerRecords<String, String> records = myConsumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : records) {
					System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s",
							record.topic(), record.partition(), record.offset(), record.key(), record.value()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}


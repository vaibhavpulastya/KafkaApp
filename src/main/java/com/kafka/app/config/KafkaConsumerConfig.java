package com.kafka.app.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {


	@Value("${kafka.bootstrap.server1}")
	private String bootstrapServer1;
	@Value("${kafka.bootstrap.server2}")
	private String bootstrapServer2;
	@Value("${kafka.bootstrap.server3}")
	private String bootstrapServer3;

	@Value("${kafka.config.consumer.count}")
	private int consumerCount;
	
	@Value("${kafka.config.consumer.group}")
	private String consumerGroup;
	

	@Bean(name = "demoKafkaListenerContainerFactory")
	public ConcurrentKafkaListenerContainerFactory<String, String> saveProspectTopicKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConcurrency(consumerCount);
		
		// different types
		// relevant ones are RECORD, BATCH, MANUAL, MANUAL_IMMEDIATE
		
		factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
		factory.setConsumerFactory(demoKafkaListenerConsumerFactory());
		return factory;
	}
	
	@Bean
	public ConsumerFactory<String, String> demoKafkaListenerConsumerFactory() {
		Map<String, Object> props = new HashMap<>();
		
		// list of URLs used to make the initial connection to obtain all the brokers in the kafka cluster 
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
				String.format("%s,%s,%s", bootstrapServer1, bootstrapServer2, bootstrapServer3));
		
		// string that identifies the consumer group this belongs to.
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
		
		// max number of records returned in a single poll, consumer caches them and returns them incrementally
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
		
		// timeout used to detect whether the consumer is dead or not
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000);
		
		// periodic heartbeats indicate the liveness of consumer to the broker
		props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
		
		// if true the consumer's offset will be committed in the background
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		
		
		// starting point of initial offset in kafka  - can be one of latest or earliest
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		
		// deserializer for the kafka payload
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		
		return new DefaultKafkaConsumerFactory<>(props);
	}
	
}
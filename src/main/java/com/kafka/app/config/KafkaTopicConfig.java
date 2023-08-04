package com.kafka.app.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicConfig {

	@Value("${kafka.bootstrap.server1}")
	private String bootstrapServer1;
	@Value("${kafka.bootstrap.server2}")
	private String bootstrapServer2;
	@Value("${kafka.bootstrap.server3}")
	private String bootstrapServer3;

	@Value("${kafka.config.topic.name}")
	private String topicName;
	
	@Value("${kafka.config.topic.partition}")
	private int partitionNum;
	
	@Value("${kafka.config.topic.replication.factor}")
	private int replicationFactor;

	@Bean
	public KafkaAdmin kafkaAdmin() {
		Map<String, Object> configs = new HashMap<String, Object>();
		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, String.format("%s,%s,%s", bootstrapServer1, bootstrapServer2, bootstrapServer3));
		return new KafkaAdmin(configs);
	}


	@Bean
	public NewTopic kafkaDemoTopic() {
		return new NewTopic(topicName, partitionNum, (short) replicationFactor);
	}

}

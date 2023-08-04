package com.kafka.app.demo;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.app.utils.KafkaConsumerUtil;
import com.kafka.app.utils.KafkaPublisher;

@RestController
public class DemoController {

	@Autowired
	KafkaPublisher kafkaPublisher;
	
	@Value("${kafka.config.topic.name}")
	private String topicName;
	
	@Autowired
	KafkaConsumerUtil kafkaConsumerUtil;

	@PostMapping(path = "/send/{message}")
	public void sendFoo(@PathVariable String message) {
		kafkaPublisher.publish(topicName, message);
	}
	
	@PostMapping(path = "/resetOffsetsForTime/{timeMillis}")
	public void resetOffsetsForTime(@PathVariable Long timeMillis) {
		kafkaConsumerUtil.resetOffsetsForTime(timeMillis);		
	}
	
}

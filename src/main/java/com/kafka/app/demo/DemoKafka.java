package com.kafka.app.demo;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import com.kafka.app.utils.KafkaPublisher;

@Service
public class DemoKafka {

	private final Logger logger = LoggerFactory.getLogger(DemoKafka.class);	

	
	@RetryableTopic(attempts = "5", backoff = @Backoff(delay = 5_000, maxDelay = 30_000, multiplier = 2))
	@KafkaListener(groupId = "${kafka.config.consumer.group}", topics = "${kafka.config.topic.name}", containerFactory = "demoKafkaListenerContainerFactory")
	public void listen(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.OFFSET) long offset, 
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long publishTs,
			Acknowledgment acknowledgment) {

		this.logger.info("CONSUMED MESSAGE={} from TOPIC={} PARTITION={} MSG_OFFSET={} publishTs={}", 
				in, topic, offset, publishTs);
		if (in.startsWith("fail")) {
			throw new RuntimeException("failed");
		}
		acknowledgment.acknowledge();
	}
	
	//DLT Annotated method must be in the same class as the corresponding KafkaListener annotation.
	@DltHandler
	public void listenDlt(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.OFFSET) long offset, Acknowledgment acknowledgment) {

		this.logger.info("CONSUMED DLT: MESSAGE={} from TOPIC={} MSG_OFFSET={}", in, topic, offset);
		acknowledgment.acknowledge();
	}

}

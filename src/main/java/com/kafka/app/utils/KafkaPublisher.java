package com.kafka.app.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class KafkaPublisher {


	private static final Logger logger = LoggerFactory.getLogger(KafkaPublisher.class);
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void publish(String topic, String message) {
		ListenableFuture<SendResult<String, String>> future= kafkaTemplate.send(topic, message);
		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

			@Override
			public void onSuccess(SendResult<String, String> result) {
				logger.info("PUBLISHED MESSAGE={} to TOPIC={} PARTITION={} MSG_OFFSET={} publishTs={}", topic, 
						result.getRecordMetadata().partition(), result.getRecordMetadata().offset(), result.getRecordMetadata().timestamp());
			}

			@Override
			public void onFailure(Throwable ex) {
				logger.warn("Error in publishing kafka message", ex);
			}

		});
	}


}
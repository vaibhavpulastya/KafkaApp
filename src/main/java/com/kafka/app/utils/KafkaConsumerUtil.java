package com.kafka.app.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

@Service
public class KafkaConsumerUtil {

	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerUtil.class);
		
	@Value("${kafka.config.topic.partition}")
	private int partitionNum;
	
	@Value("${kafka.config.topic.name}")
	private String topicName;
	
	@Value("${kafka.bootstrap.server1}")
	private String bootstrapServer1;

	@Value("${kafka.config.consumer.group}")
	private String consumerGroup;


	ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);;
	
	public void resetOffsetsForTime(long searchTimeMillis){
		try {

			logger.info("Searching for timestamp={}", searchTimeMillis);
			
			KafkaConsumer<String, String> kafkaConsumer = getKafkaConsumer();
			
		    List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topicName);
		    
		    logger.info("partitionInfo size={}", partitionInfos.size());
		    
		    List<TopicPartition> topicPartitions = partitionInfos
		            .stream()
		            .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
		            .collect(Collectors.toList());

			
			Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
			for(TopicPartition tp : topicPartitions) {
				timestampsToSearch.put(tp, searchTimeMillis);
				logger.info("topic partition {} | {}", tp.topic(), tp.partition());
			}

			Map<TopicPartition, OffsetAndTimestamp> result = kafkaConsumer.offsetsForTimes(timestampsToSearch);
			logger.info("OffsetsForTimes Response Received: {}", mapper.writeValueAsString(result));
			
			for(TopicPartition tp: result.keySet()) {
				if(result.get(tp) != null) {
					logger.info("Offset Updated for partition={} topic={} offset={} partition={}", tp.partition(), tp.topic()
							, result.get(tp).offset(), result.get(tp).timestamp());
					kafkaConsumer.seek(tp, result.get(tp).offset());
					logger.info("Offset Updated for partition={} topic={}", tp.partition(), tp.topic());
				}
			}
			 
		}
		catch(Exception e) {
			logger.error("ERROR in resetOffsetsForTime", e);
		}
	}
	
	public KafkaConsumer<String, String> getKafkaConsumer() {
	    Map<String, Object> kafkaParams = new HashMap<>();
	    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer1);
	    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
	    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	    kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
	    kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
	    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaParams);
	    return consumer;
	}
}

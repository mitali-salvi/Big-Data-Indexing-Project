package com.csye7255.project.service;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.client.RequestOptions;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.csye7255.project.Exception.ExpectationFailed;
import com.csye7255.project.repo.ElasticSearchRepository;

@Service
public class KafkaService {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Value("${spring.kafka.topic.name}")
	private String topicName;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private ElasticSearchRepository elasticSearchRepo;

	/**
	 * Publish message on Kafka topic
	 *
	 * @param message
	 */
	public void publish(String message, String operation) throws Exception {
		
		try {
			elasticSearchRepo.getClient().ping(RequestOptions.DEFAULT);
		} catch (Exception e) {
			logger.error("ElasticSearch is not reachable");
			throw new Exception("ElasticSearch is not reachable");
		}

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, operation, message);
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
		 
		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

			@Override
			public void onSuccess(SendResult<String, String> result) {
			}

			@Override
			public void onFailure(Throwable ex) {
			}
		});
		
	}
	
//	https://stackoverflow.com/a/56099158
//	https://github.com/spring-projects/spring-kafka/issues/1189
//	RABBIT-MQ: https://stackoverflow.com/questions/24784622/request-response-pattern-using-spring-amqp-library

	@KafkaListener(topics = "kafkatopic", groupId = "group_id")
	public void consume(ConsumerRecord<String, String> record)  {
		logger.info("Consumed Message Key: " + record.key().toString());
		logger.info("Consumed Message Value: " + record.value().toString());

		// Send Message to elastic search
		if (record.key().toString().equals("post")) {
			logger.info("Saving data in ES");
			JSONObject jsonData = new JSONObject(
					new JSONTokener((new JSONObject(record.value().toString())).toString()));
			try {
				if (validateJson(jsonData)) {
					String key = jsonData.get("objectType") + "_" + jsonData.get("objectId");
					elasticSearchRepo.index(key, record.value().toString());
				}
			} catch (FileNotFoundException e) {
				logger.error("FileNotFoundException");
			} catch (JSONException e) {
				logger.error("JSONExcpetion"+ e.getMessage());
			}
			catch (Exception e) {
				logger.error("Kafka Listener exception while indexing:::"+ e.getMessage());
			}
		} else if (record.key().toString().equals("patch")) {
			logger.info("Patching data in ES");
			JSONObject jsonData = new JSONObject(
					new JSONTokener((new JSONObject(record.value().toString())).toString()));
			String key = jsonData.get("objectType") + "_" + jsonData.get("objectId");
			try {
				elasticSearchRepo.index(key, record.value().toString());
			} catch (Exception e) {
				logger.error("Kafka Listener exception while indexing:::"+ e.getMessage());

			}

		} else if (record.key().toString().equals("delete")) {
			logger.info("Deleting data from ES");
			try {
				elasticSearchRepo.delete(record.value().toString());
			} catch (Exception e) {
				logger.error("Kafka Listener exception while indexing:::"+ e.getMessage());
			}
		}
	}

	public boolean validateJson(JSONObject jsonData) throws FileNotFoundException {
		logger.info("Validating JSONschema");
		BufferedReader bufferedReader = new BufferedReader(new FileReader("src/main/resources/static/jsonschema.json"));
		JSONObject jsonSchema = new JSONObject(new JSONTokener(bufferedReader));
		System.out.println(jsonSchema.toString());
		Schema schema = SchemaLoader.load(jsonSchema);
		try {
			schema.validate(jsonData);
			return true;
		} catch (ValidationException e) {
			throw new ExpectationFailed("Enter correct input! The issue is present in " + e.getMessage());
		}
	}
}

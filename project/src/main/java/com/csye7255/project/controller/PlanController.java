package com.csye7255.project.controller;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.csye7255.project.Etag.EtagMap;
import com.csye7255.project.Exception.BadRequest;
import com.csye7255.project.Exception.ExpectationFailed;
import com.csye7255.project.Exception.Forbidden;
import com.csye7255.project.Exception.Notmodified;
import com.csye7255.project.Exception.PreconditionFailed;
import com.csye7255.project.service.KafkaService;
import com.csye7255.project.service.PlanService;
import com.csye7255.project.service.TokenService;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@RestController
@RequestMapping
public class PlanController {

	private static final Logger logger = LoggerFactory.getLogger(PlanController.class);

	@Autowired
	private PlanService planService;

	@Autowired
	private TokenService tokenService;

	@Value("${spring.redis.host}")
	String redisHost;

	@Autowired
	private KafkaService kafkaService;

	private static JedisPool jedisPool = new JedisPool("localhost", 6379);

	@PostMapping(path = "/plan", consumes = "application/json", produces = "application/json")
	@ResponseBody
	public ResponseEntity<String> addInsurancePlan(@RequestHeader HttpHeaders headers, @RequestBody String plan) {
		logger.info("POST mapping");

		String token = headers.getFirst("Authorization");
		if (!(tokenService.validateToken(token)))
			throw new BadRequest("Token is expired");
		Map<String, String> validEtag = planService.createPlan(token, plan);
		String etag = validEtag.get("etag");
		logger.info("Etag: " + etag);

		try{
			logger.info("POST - Publishing to KAFKA");
			kafkaService.publish(plan, "post");
		}
		catch (Exception e) {
			logger.error("Kafka OR ES is shut down");
			logger.error("Deleting from Redis");
			planService.deleteData(validEtag.get("planid"));
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).eTag(etag)
					.body("Error in pushing to queue or elastic search");
		}


		if (validEtag.size() > 1) {
			logger.info("Data saved successfully");
			return ResponseEntity.status(HttpStatus.CREATED).eTag(etag)
					.body("Data Saved. PlanId: " + validEtag.get("planid"));
		} else {
			logger.error("Data already present");
			return ResponseEntity.status(HttpStatus.ALREADY_REPORTED).eTag(etag).body("Data already present");
		}
	}

	@GetMapping(path = "/plan/{id}", produces = "application/json")
	public ResponseEntity<String> getInsurancePlan(@PathVariable(value = "id") String planId,
			@RequestHeader HttpHeaders header) {
		logger.info("GET mapping");
		try {
			String token = header.getFirst("Authorization");
			if (!(tokenService.validateToken(token)))
				throw new BadRequest("Token is expired");

			if (planId != null) {
				String etag = header.getETag();
				Map<String, String> validEtag = planService.getPlan(token, planId, etag);
				logger.info("Got data from Redis");
				return ResponseEntity.status(HttpStatus.OK).eTag(validEtag.get("etag")).body(validEtag.get("plan"));
			} else {
				logger.error("Plan does not exist");
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).eTag(planService.getEtags(planId))
						.body("PlanId not present");
			}
		} catch (BadRequest ex) {
			logger.error("BadRequest");
			return ResponseEntity.status(ex.getStatus()).eTag(planService.getEtags(planId)).body(ex.getMessage());
		} catch (Forbidden ex) {
			logger.error("Forbidden");
			return ResponseEntity.status(ex.getStatus()).eTag(planService.getEtags(planId)).body(ex.getMessage());
		} catch (PreconditionFailed ex) {
			logger.error("PreconditionFailed");
			return ResponseEntity.status(ex.getStatus()).eTag(planService.getEtags(planId)).body(ex.getMessage());
		} catch (Notmodified ex) {
			logger.error("Notmodified");
			return ResponseEntity.status(ex.getStatus()).eTag(planService.getEtags(planId)).body(ex.getMessage());
		}
	}

	@DeleteMapping(path = "/plan/{id}")
	public ResponseEntity<String> deletePlan(@PathVariable(value = "id") String planId,
			@RequestHeader HttpHeaders headers) {
		logger.info("DELETE mapping");
		String token = headers.getFirst("Authorization");
		if (!(tokenService.validateToken(token)))
			throw new BadRequest("Token is expired");
		if (planId != null) {
			if ((planService.deleteData(planId)) > 0) {
				planService.removeEtags(planId);

				try {
					logger.info("Deleting plan from Kafka");
					kafkaService.publish(planId, "delete");
				}
				catch (Exception e) {
					logger.info("Kafka OR ES is shut down");
					return ResponseEntity.status(HttpStatus.BAD_REQUEST)
							.body("Error in pushing to queue or elastic search");
				}

				logger.info("Deleted plan successfully");
				return ResponseEntity.status(HttpStatus.OK).body("Delete Successful!");
			} else {
				logger.error("Plan ID not found to be deleted");
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Plan Id Not Found");
			}
		} else {
			logger.error("Plan ID not entered");
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Enter the plan ID!!");
		}
	}

	@PutMapping(path = "/plan/{id}")
	public ResponseEntity<String> updatePlan(@PathVariable(value = "id") String planId, @RequestBody String plan,
			@RequestHeader HttpHeaders header) {
		logger.info("PUT mapping");
		try {
			String token = header.getFirst("Authorization");
			if (!(tokenService.validateToken(token)))
				throw new BadRequest("Token is expired");
			String etag = null;
			if (header.getIfMatch().get(0) != null) {
				etag = header.getIfMatch().get(0);
				etag = etag.replace("\"", "");
			}
			logger.info("Etag: " + etag);
			Map<String, String> validEtag = planService.updatePlan(token, planId, etag, plan);

			try {
				logger.info("PUT - Publishing data to Kafka: ");
				kafkaService.publish(plan, "post");
			}
			catch (Exception e) {
				logger.info("Kafka OR ES is shut down");
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).eTag(etag)
						.body("Error in pushing to queue or elastic search");
			}

			etag = validEtag.get("etag");
			if (validEtag.size() > 1) {
				logger.info("Data updated successfully");
				return ResponseEntity.status(HttpStatus.OK).eTag(etag)
						.body("Data Saved. Plan id:" + validEtag.get("planid"));
			} else {
				logger.error("Data not updated");
				return ResponseEntity.status(HttpStatus.NOT_MODIFIED).eTag(etag).body("Data not updated");
			}
		} catch (BadRequest ex) {
			logger.error("BadRequest");
			return ResponseEntity.status(ex.getStatus()).eTag(planService.getEtags(planId)).body(ex.getMessage());
		} catch (Forbidden ex) {
			logger.error("Forbideen");
			return ResponseEntity.status(ex.getStatus()).eTag(planService.getEtags(planId)).body(ex.getMessage());
		} catch (PreconditionFailed ex) {
			logger.error("PreconditionFailed");
			return ResponseEntity.status(ex.getStatus()).eTag(planService.getEtags(planId)).body(ex.getMessage());
		} catch (Notmodified ex) {
			logger.error("Notmodified");
			return ResponseEntity.status(ex.getStatus()).eTag(planService.getEtags(planId)).body(ex.getMessage());
		}
	}

	@PatchMapping(path = "/plan/{id}", produces = "application/json")
	public ResponseEntity<String> patchPlan1(@PathVariable(value = "id") String planId, @RequestBody String plan,
			@RequestHeader HttpHeaders headers) throws Exception {
		logger.info("PATCH mapping");
		Jedis jedis = jedisPool.getResource();
		try {
			String etag = null;

			String token = headers.getFirst("Authorization");
			if (!(tokenService.validateToken(token)))
				throw new BadRequest("Token is expired");
			if (headers.getIfMatch().size() > 0) {
				if (headers.getIfMatch().get(0) != null) {
					etag = headers.getIfMatch().get(0);
					etag = etag.replace("\"", "");
				}
			} else {
				throw new Forbidden("ETag is not present");
			}

			if (EtagMap.getEtags().containsKey(planId + "p")) {
				if (!etag.equals(EtagMap.getEtags().get(planId + "p")))
					throw new Forbidden("ETag not matched");
			}
			JSONObject input = new JSONObject(plan);

			if (!planId.contains("plan_") || !input.keySet().contains("objectId")) {
				JSONObject data = new JSONObject(jedis.get(planId));
				for (Object key : input.keySet()) {
					if (!data.get((String) key).equals(input.get((String) key))) {
						data.put(key.toString(), input.get((String) key));
					}
				}
				
				jedis.set(planId, data.toString());

				logger.info("PUT - Publishing data to Kafka: ");
				kafkaService.publish(data.toString(), "post");

				String skey = null;
				Set<String> set = EtagMap.getEtags().keySet().stream().filter(s -> s.endsWith("p"))
						.collect(Collectors.toSet());
				if (!set.isEmpty()) {
					EtagMap.getEtags().keySet().removeAll(set);
					for (String s : set) {
						skey = s;
					}
				}

				etag = UUID.randomUUID().toString();
				EtagMap.getEtags().put(skey, etag);

			} else {
				if (input.getString("objectType").equalsIgnoreCase("planservice")) {
					String message = validateJson1(input);
					if (message != null) {
						return ResponseEntity.status(HttpStatus.BAD_REQUEST)
								.body("Schema not validated. Please check " + message);
					}
					Map<String, String> data = PlanService.retrieveMap(input);
					for (Map.Entry<String, String> entry : data.entrySet()) {
						jedis.set((String) entry.getKey(), (String) entry.getValue());
					}
					String fullPlan = jedis.get(planId);

					String[] split = fullPlan.split("\\[");
					String x = "[\\\"" + input.getString("objectType") + "_" + input.getString("objectId") + "\\\",";
					fullPlan = split[0] + x + split[1];
					jedis.set(planId, fullPlan);
					planService.removeEtags(planId);
					etag = UUID.randomUUID().toString();
					EtagMap.getEtags().put(planId + "p", etag);

					logger.info("PUT - Publishing data to Kafka: ");
					kafkaService.publish(planService.readData(fullPlan).toString(), "patch");
				}

			}
			jedis.close();

			logger.info("Plan patched successfully");
			return ResponseEntity.status(HttpStatus.OK).eTag(etag).body("Data Saved. Plan id: " + planId);
		} catch (BadRequest ex) {
			logger.error("BadRequest");
			return ResponseEntity.status(ex.getStatus()).eTag(planService.getEtags(planId)).body(ex.getMessage());
		} catch (Forbidden ex) {
			logger.error("Forbidden");
			return ResponseEntity.status(ex.getStatus()).eTag(planService.getEtags(planId)).body(ex.getMessage());
		} catch (PreconditionFailed ex) {
			logger.error("Precondition");
			return ResponseEntity.status(ex.getStatus()).eTag(planService.getEtags(planId)).body(ex.getMessage());
		} catch (Notmodified ex) {
			logger.error("Notmodified");
			return ResponseEntity.status(ex.getStatus()).eTag(planService.getEtags(planId)).body(ex.getMessage());
		} catch (Exception ex) {
			logger.error("Exception");
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).eTag(planService.getEtags(planId)).body(ex.getMessage());
		}finally {
			if (jedis != null)
				jedis.close();
		}
	}

	public String validateJson1(JSONObject jsonData) throws Exception {
		BufferedReader bufferedReader = new BufferedReader(new FileReader("src/main/resources/static/Service.json"));
		JSONObject jsonSchema = new JSONObject(new JSONTokener(bufferedReader));
		Schema schema = SchemaLoader.load(jsonSchema);
		try {
			schema.validate(jsonData);
			return null;
		} catch (ValidationException e) {
			throw new ExpectationFailed("Enter correct input! The issue is present in " + e.getMessage());
		}
	}
}

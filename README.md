# Big Data Indexing Final Project <br/>

### Project Description
The project validates a JSON based on the JSON schema and stores in the in-memory database Redis. 
Also on successful storage in Redis, The Kafka messaging queue will pass the JSON to elastic-search for indexing. 
The elastic-search indexing can be verified by using Kibana to query to index

### Pre-requisite
Install and set up the following: 
1. Redis (https://redis.io/topics/quickstart) 
2. Kafka (https://kafka.apache.org/quickstart) 
3. Elastic-Search (https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html) 
4. Kibana (https://www.elastic.co/guide/en/kibana/current/install.html)

### Run the application 

#### Start Redis Service using the following command: 
1. Go to redis folder
2. Run src/redis-server 

#### Start Kafka Service using the following command: 
1. Go to kafka folder
2. Run bin/zookeeper-server-start.sh config/zookeeper.properties
3. Run bin/kafka-server-start.sh config/server.properties 

#### Start Elastic Search Service using the following command: 
1. Go to elastic-search folder
2. Run bin/elasticsearch 

#### Start Kibana Search Service using the following command: 
1. Go to kibana folder
2. Run bin/kibana

#### Run the Spring Boot folder
1. Go to project root folder  
2. Run 'mvn spring-boot:run'
3. Open Postman and post a JSON to validate the JSON 
4. Open Kibana portal and run query to search the index for input JSON 

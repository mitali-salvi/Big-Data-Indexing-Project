package com.csye7255.project.repo;

import java.io.IOException;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

@Repository
public class ElasticSearchRepository {

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRepository.class);

	private RestHighLevelClient restHighLevelClient;

	public ElasticSearchRepository(RestHighLevelClient restHighLevelClient) {
		this.restHighLevelClient = restHighLevelClient;
	}

	public void index(String id, String document) throws Exception{
		logger.info("Indexing document in ES:" + id);
		IndexRequest request = new IndexRequest("plan", "_doc", id);
		request.source(document, XContentType.JSON);
		try {
			restHighLevelClient.index(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
		catch (Exception e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
	}

	public void delete(String id) throws Exception {
		logger.info("Deleting document in ES:" + id);
		try {
			DeleteRequest deleteRequest = new DeleteRequest("plan", "_doc", id);
			restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
		catch (Exception e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
	}
	
	public RestHighLevelClient getClient() {
		
		return restHighLevelClient;
	}
}


package com.krishagni.sample.rest;

import java.io.File;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.multipart.MultiPart;
import com.sun.jersey.multipart.file.FileDataBodyPart;

public class ObjectImporter {
	private static Client client;
	
	private static Logger logger = Logger.getLogger(ObjectImporter.class.getName());
	
	public boolean importObjects(Map<String, String> props) {
		String path = props.get("file");

		ClientResponse response = null;
		ClientConfig config = new DefaultClientConfig();
		config.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
		client = Client.create(config);
		client.addFilter(new com.sun.jersey.api.client.filter.HTTPBasicAuthFilter(BulkImporter.USER_NAME, BulkImporter.PASSWORD));
		WebResource service = client.resource(BulkImporter.APP_URL + "/import-jobs/input-file");
		
		MultiPart multiPart = new MultiPart();
		multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);
		FileDataBodyPart filePart = new FileDataBodyPart("file", new File(path), MediaType.APPLICATION_OCTET_STREAM_TYPE);
		multiPart.bodyPart(filePart);
		
		try {
			response = service.type(MediaType.MULTIPART_FORM_DATA).post(ClientResponse.class, multiPart);
			logger.info(service.getURI().toString() + " Status: " + response.getStatus());
			if(response.getStatus() != 200) {
				logger.info("Error" + response.getEntity(String.class));
				return false;
			}
			
			String fileId = new JSONObject(response.getEntity(String.class)).getString("fileId");
			
			service = client.resource(BulkImporter.APP_URL + "/import-jobs");
			JSONObject detail = getData(props);
			detail.put("inputFileId", fileId);
			response = service.type(MediaType.APPLICATION_JSON).post(ClientResponse.class, detail.toString());
			logger.info(service.getURI().toString() + " Status: " + response.getStatus());

			if(response.getStatus() != 200) {
				logger.info("Error" + response.getEntity(String.class));
				return false;
			}
			
			Long jobId = new JSONObject(response.getEntity(String.class)).getLong("id");
			return importSucceeded(jobId);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			logger.error(e);
			return false;
		}
	}
	
	public static boolean importSucceeded(Long jobId) throws InterruptedException {
		JSONObject jobDetail = getJobRun(jobId);
		String status = jobDetail.getString("status");
		long sleepTime = BulkImporter.TIME_SPAN * 100;
		
		while (StringUtils.equals(status, "IN_PROGRESS")) {
			jobDetail = getJobRun(jobId);
			status = jobDetail.getString("status");
			if(!StringUtils.equals(status, "IN_PROGRESS")) {
				break;
			}
			
			Thread.sleep(sleepTime);
		}
		
		if(StringUtils.equals(status, "COMPLETED")) {
			Long failedCount = jobDetail.getLong("failedRecords");
			if(failedCount == 0) {
				return true;
			}
		}
		
		return false;
	}
	
	public static JSONObject getJobRun(Long jodId) {
		ClientResponse response = null;
		ClientConfig config = new DefaultClientConfig();
		config.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
		client = Client.create(config);
		client.addFilter(new com.sun.jersey.api.client.filter.HTTPBasicAuthFilter(BulkImporter.USER_NAME, BulkImporter.PASSWORD));
		WebResource service = client.resource(BulkImporter.APP_URL + "/import-jobs/" + jodId);
		response = service.type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
		logger.info(service.getURI().toString() + " Status: " + response.getStatus());
		
		if (response.getStatus() != 200) {
			logger.info("Error" + response.getEntity(String.class));
			return null;
		}
		
		return new JSONObject(response.getEntity(String.class));
	}
	
	public static JSONObject getData(Map<String, String> props) {
		String objType = props.get("object_name");
		String entityType = props.get("entity_type");
		String formName = props.get("form_name");
		String csvType = StringUtils.equalsIgnoreCase(objType, "shipment") ? "MULTIPLE_ROWS_PER_OBJ" : "SINGLE_ROW_PER_OBJ";
		String importType = StringUtils.equalsIgnoreCase(props.get("is_update"), "update") ? "UPDATE" : "CREATE";

		JSONObject detail = new JSONObject();
		detail.put("csvType", csvType);
		detail.put("importType", importType);
		detail.put("objectType", objType);
		
		if (StringUtils.isNotBlank(entityType) && StringUtils.isNotBlank(formName)) {
			JSONObject objectParams = new JSONObject();
			objectParams.put("entityType", entityType);
			objectParams.put("formName", formName);
			detail.put("objectParams", objectParams);
			detail.put("objectType", "extensions");
		}
		return detail;
	}
}

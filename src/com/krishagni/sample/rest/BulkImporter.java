package com.krishagni.sample.rest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.core.UriBuilder;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

public class BulkImporter {
	
	public static String USER_NAME;
	
	public static String PASSWORD;
	
	public static String APP_URL;
	
	public static String MAIN_FILE;
	
	public static Long TIME_SPAN;
	
	private static Logger logger = Logger.getLogger(BulkImporter.class.getName());
	
	private static Map<String, String> objectTypes = new HashMap<String, String>();
	
	private static URI getBaseURI(String app_url) {
		return UriBuilder.fromUri(app_url + "/rest/ng").build();
	}
	
	public static void main(String args[]) throws Exception {
		setProperties();
		List<String[]> dataToWrite = new ArrayList<String[]>();
		File filePath = new File(MAIN_FILE);
      
		CSVReader csvReader = null;
		try {
			FileInputStream file = new FileInputStream(filePath);
			csvReader = new CSVReader(new InputStreamReader(file));
			String[] row = null;
			row = csvReader.readNext();
			
			String[] header = new String[row.length + 1];
			System.arraycopy(row, 0, header, 0, row.length);
			header[2] = "Status";
			dataToWrite.add(header);
			
			while(true) {
				EmailService email = new EmailService();
				String status = "Success";
				Map<String, String> props = new HashMap<String, String>();
				
				row = csvReader.readNext();
				if(row == null) {
					break;
				}
				
				if(StringUtils.isBlank(row[1])) {
					continue;
				}
				
				String[] columns = new String[row.length + 1];
				System.arraycopy(row, 0, columns, 0, row.length);
				
				String object = columns[0].toLowerCase().trim();
				object = objectTypes.containsKey(object) ? objectTypes.get(object) : object;
				String[] eventDetails = getEventName(columns[1]);
				
				props.put("object_name", object);
				props.put("file", columns[1]);
				props.put("entity_type", eventDetails != null ? eventDetails[1] : null);
				props.put("form_name", eventDetails != null ? eventDetails[0] : null);
				props.put("is_update", StringUtils.isNotBlank(row[2]) ? row[2].trim() : null);
				
				logger.info(columns[0] + "Import Started...");
				
				ObjectImporter importer = new ObjectImporter();
				
				if(!importer.importObjects(props)) {
					status = "Failed";
					logger.error("Error:: " + columns[0] + " Import Failed...");
				}
				email.sendMail(status, columns[0]);
				
				logger.info(columns[0] + "Import Completed...");
				columns[2] = status;
				dataToWrite.add(columns);
				if(StringUtils.equalsIgnoreCase(status, "failed")) {
					break;
				}
			}
			
			String outputFile = MAIN_FILE.substring(0, MAIN_FILE.lastIndexOf('/')) + "/output.csv";
			CSVWriter writer = new CSVWriter(new FileWriter(outputFile));
			for (String[] line : dataToWrite) {
				writer.writeNext(line);
			}
			
			writer.close();
			System.out.println("Execution Completed");
		} catch (FileNotFoundException ex) {
			logger.error("File Not Found" + ex.getMessage());
			logger.error(ex);
		} catch (Exception ex) {
			logger.error(ex);
		}
	}
	
	private static String[] getEventName(String file) {
		String fileName = file.substring(file.lastIndexOf('/') + 1, file.lastIndexOf('.')).trim();
		
		if (!StringUtils.contains(fileName, "_")) {
			return null;
		}
		
		String[] names = new String[2];
		names = fileName.split("_");
		return names;
	}

	private static void setProperties() throws FileNotFoundException, IOException {
		Properties props = new Properties();
		props.load(new FileInputStream(new File("build.properties")));
			
		USER_NAME = props.getProperty("loginname").trim();
		PASSWORD = props.getProperty("password").trim();
		APP_URL = getBaseURI(props.getProperty("app_url")).toString().trim();
		MAIN_FILE = props.getProperty("file_path").trim();
		TIME_SPAN = Long.parseLong(props.getProperty("time_span").trim());
		
		EmailService.from = props.getProperty("email_id").trim();
		EmailService.to = props.getProperty("to").trim();
		EmailService.username = props.getProperty("email_id").trim();
		EmailService.password = props.getProperty("email_password").trim();
		feedMap();
	}
	
	private static void feedMap() {
		objectTypes.put("participant", "cpr");
		objectTypes.put("derivative", "specimenDerivative");
		objectTypes.put("aliquot", "specimenAliquot");
		objectTypes.put("containers", "storageContainer");
		objectTypes.put("sraliquot", "srAliquot");
		objectTypes.put("srderivative", "srDerivative");
	}
}
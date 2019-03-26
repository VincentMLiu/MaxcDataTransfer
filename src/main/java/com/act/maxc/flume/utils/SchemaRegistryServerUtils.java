package com.act.maxc.flume.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

import org.apache.avro.Schema;

import com.alibaba.fastjson.JSONObject;

public class SchemaRegistryServerUtils {

	
	
	  public static Schema getSchema(String serverUrl, String subject) {
		  Schema.Parser parser = new Schema.Parser();
	      StringBuilder result = new StringBuilder();
	      URL url;
		try {
			url = new URL(serverUrl + "/subjects/" + subject + "/versions/latest");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			rd.close();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (ProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
//	    json format: {"subject":"topic1","version":1,"id":102,"schema":"{\"type\":\"record\",\"name\":\"topic1\",\"fields\":[{\"name\":\"c1\",\"type\":\"string\"},{\"name\":\"c2\",\"type\":\"string\"},{\"name\":\"c3\",\"type\":\"int\"}]}"}
	      JSONObject json = JSONObject.parseObject(result.toString());
	      String schemaStr = (String) json.get("schema");
	      Schema schema = parser.parse(schemaStr);
	      System.out.println(schema);
		  return schema;
	  }
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

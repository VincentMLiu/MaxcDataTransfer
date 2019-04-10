/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.act.maxc.flume.sources.http.handler;

import com.act.maxc.flume.utils.JsonAvroUtils;
import com.act.maxc.flume.utils.SchemaRegistryServerUtils;
import com.google.common.base.Preconditions;

import org.apache.avro.Schema;
import org.apache.commons.collections.EnumerationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.LogPrivacyUtil;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * Binary2EventsHandler for HTTPSource that accepts any binary stream of data as event.
 * 
 * Get Schema from Confluent Registry Server, and transform binary records into seperated events.
 *
 */
public class Binary2EventsHandler implements HTTPSourceHandler {

  private static final Logger LOG = LoggerFactory.getLogger(com.act.maxc.flume.sources.http.handler.Binary2EventsHandler.class);

  //必填消息头
  private String commaSeparatedHeaders;

  private String[] mandatoryHeaders;

  public static final String MANDATORY_PARAMETERS = "mandatoryParameters";

  public static final String DEFAULT_MANDATORY_PARAMETERS = "topic,messageFormat";
  
  public static final String PARAMETER_SEPARATOR = ",";
  
  //Confluent Registry Server 默认的 serverUrl
  private String serverUrl;
  
  //入库topic
  private String topic; 
  //获取schema
  private static Schema schema;
  
  //消息格式: json、avro、csv
  private String messageFormat;
  
  //如果解析csv还需要拆分符号
  private String splitRegex = ",";

  
  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
public List<Event> getEvents(HttpServletRequest request) throws Exception {

	  //event header
	  Map<String, String> headers = new HashMap<String, String>();

    //消息头添加
    List<String> headersList = EnumerationUtils.toList( request.getHeaderNames());
    for(String header :headersList) {
    	String headerValue = request.getHeader(header);
        if (LOG.isDebugEnabled() && LogPrivacyUtil.allowLogRawData()) {
        	LOG.debug("Setting Header [Key, Value] as [{" +header + "},{" +headerValue + "}] ");
          }
        headers.put(header, headerValue);
    }

	  //消息头header需要必填的属性
	  for (String header : mandatoryHeaders) {
		  Preconditions.checkArgument(headers.containsKey(header),
				  "Please specify " + header + " parameter in the request.");
	  }
    
	  dealRequstHeaders(request);
    headers.put("schema", schema.toString());
	//处理消息头
    //属性添加
    Map<String, String[]> parameters = request.getParameterMap();
    for (String parameter : parameters.keySet()) {
      String value = parameters.get(parameter)[0];
      if (LOG.isDebugEnabled() && LogPrivacyUtil.allowLogRawData()) {
        LOG.debug("Setting Header [Key, Value] as [{},{}] ", parameter, value);
      }
      headers.put(parameter, value);
    }

    InputStream inputStream = request.getInputStream();

    
    try {
    	List<Event> eventList = new ArrayList<Event>();
    	
    	switch (messageFormat) {
    	case "json":
    		eventList.addAll(JsonAvroUtils.jsonInputStreamToAvroEventList(inputStream, schema, headers));
    		break;
    	case "csv":
    		eventList.addAll(JsonAvroUtils.csvInputStreamToAvroEventList(inputStream, splitRegex,schema, headers));
    		break;
    	case "avro":
    		eventList.addAll(JsonAvroUtils.avroInputStreamToAvroEventList(inputStream, schema, headers));
    		break;
    	
    	
    	}
    	
      LOG.info("Building an Event List with size -- {}", eventList.size());
      return eventList;
    } finally {
      inputStream.close();
    }
  }

  
  private void dealRequstHeaders(HttpServletRequest request) {
		//需要入库的topic
		topic = request.getHeader("topic");
		if(StringUtils.isNotBlank(request.getHeader("serverUrl"))) {
			serverUrl = request.getHeader("serverUrl");
		}
		schema = SchemaRegistryServerUtils.getSchema(serverUrl, topic);
		//消息格式
		messageFormat = request.getHeader("messageFormat");
		if(StringUtils.equalsIgnoreCase(messageFormat, "csv")) {
			if(StringUtils.isNotBlank(request.getHeader("splitRegex"))) {
				splitRegex = request.getHeader("splitRegex"); 
			}else {
				LOG.info("User doesn't specify splitRegex in 'csv' messageFormat, will use [,] as default splitRegex...");	
			}
			
		}
	}
  
  public void configure(Context context) {
	  this.serverUrl = context.getString("Binary2EventsHandler.serverUrl", "http://localhost:58088");
	  
	    this.commaSeparatedHeaders = context.getString(MANDATORY_PARAMETERS,
                DEFAULT_MANDATORY_PARAMETERS);
	    this.mandatoryHeaders = commaSeparatedHeaders.split(PARAMETER_SEPARATOR);
	  
  }

}

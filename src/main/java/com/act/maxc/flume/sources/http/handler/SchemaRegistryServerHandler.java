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
import org.apache.flume.event.EventBuilder;
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
 * SchemaRegistryServerHandler for HTTPSource that accepts formated binary stream of data as event.
 *
 */
public class SchemaRegistryServerHandler implements HTTPSourceHandler {

  private static final Logger LOG = LoggerFactory.getLogger(org.apache.flume.source.http.BLOBHandler.class);

  private String commaSeparatedHeaders;

  private String[] mandatoryHeaders;
  
  private String serverUrl;

  public static final String MANDATORY_PARAMETERS = "mandatoryParameters";

  public static final String DEFAULT_MANDATORY_PARAMETERS = "";

  public static final String PARAMETER_SEPARATOR = ",";

  //入库topic
  private String topic; 
  //获取schema
  private static Schema schema;
  
  //消息格式: json、avro、csv
  private String messageFormat;
  
  //如果解析csv还需要拆分符号
  private String splitRegex;
  
  
private void dealRequstHeaders(HttpServletRequest request) {
	//需要入库的topic
	topic = request.getHeader("topic");
	schema = SchemaRegistryServerUtils.getSchema(serverUrl, topic);
	//消息格式
	messageFormat = request.getHeader("messageFormat");
	if(StringUtils.equalsIgnoreCase(messageFormat, "csv")) {
		splitRegex = request.getHeader("splitRegex"); 
	}
}
  
public List<Event> getEvents(HttpServletRequest request) throws Exception {
    //处理消息头
    dealRequstHeaders(request);
    //event header
    Map<String, String> headers = new HashMap<String, String>();
    Map<String, String[]> parameters = request.getParameterMap();
    List<String> headersList = EnumerationUtils.toList( request.getHeaderNames());
    
    for(String header :headersList) {
    	String headerValue = request.getHeader(header);
    	headers.put(header, headerValue);
    	System.out.println("Setting Header [Key, Value] as [{" +header + "},{" +headerValue + "}] ");
    }
    
    for (String parameter : parameters.keySet()) {
    	String value = parameters.get(parameter)[0];
    	System.out.println("Setting Header [Key, Value] as [{" +parameter + "},{" +value + "}] ");
    	if (LOG.isDebugEnabled() && LogPrivacyUtil.allowLogRawData()) {
    		LOG.debug("Setting Header [Key, Value] as [{},{}] ", parameter, value);
    	}
    	headers.put(parameter, value);
    }

    //消息头header需要必填的属性
//    for (String header : mandatoryHeaders) {
//      Preconditions.checkArgument(headers.containsKey(header),
//          "Please specify " + header + " parameter in the request.");
//    }
    
    headers.put("schema", schema.toString());
    //处理消息体body
    //eventBody
    InputStream inputStream = request.getInputStream();
    byte[] avroSerialBody = null;
    try {
    	if(StringUtils.equalsIgnoreCase(messageFormat, "json")) {
    		avroSerialBody = JsonAvroUtils.jsonToAvro(inputStream, schema);
    	}else if (StringUtils.equalsIgnoreCase(messageFormat, "avro")) {
    		avroSerialBody = JsonAvroUtils.avroToAvro(inputStream, schema);
    	}else if (StringUtils.equalsIgnoreCase(messageFormat, "csv")) {
    		avroSerialBody = JsonAvroUtils.csvToAvro(inputStream,  splitRegex, schema);
    	}
    } catch (Exception e) {
        e.printStackTrace();
    }

    try {
      LOG.debug("Building an Event with byte of size -- {}", avroSerialBody.length);
      Event event = EventBuilder.withBody(avroSerialBody, headers);
      event.setHeaders(headers);
      System.out.println(headers);
      List<Event> eventList = new ArrayList<Event>();
      eventList.add(event);
      return eventList;
    } finally {
      inputStream.close();
    }
  }

  public void configure(Context context) {
    this.commaSeparatedHeaders = context.getString(MANDATORY_PARAMETERS,
                                                   DEFAULT_MANDATORY_PARAMETERS);
    this.mandatoryHeaders = commaSeparatedHeaders.split(PARAMETER_SEPARATOR);
    
    
    this.serverUrl = context.getString("SchemaRegistryServerHandler.serverUrl", "http://localhost:58088");
  }

  
  public static void main(String[] args) {
//	  SchemaRegistryServerHandler sr = new SchemaRegistryServerHandler();
//	  try {
//		  schema  = sr.getSchema("transactions-value");
//	} catch (Exception e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
	  

  }
  
}

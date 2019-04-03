/**
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

package com.act.maxc.flume.interceptors;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.act.maxc.flume.utils.SchemaRegistryServerUtils;
import com.google.common.base.Optional;

/**
 * 
 * This interceptor aims to drop records which contains null in nonNullFields
 *
 *
 * Properties:<p>
 *
 *   nonNullFields: fields if contains null, will drop on-flight<p>
 *
 *
 * Sample config:<p>
 *
 * <code>
 *   agent.sources.r1.channels = c1<p>
 *   agent.sources.r1.type = http<p>
 *   agent.sources.r1.interceptors = i1<p>
 *   agent.sources.r1.interceptors.i1.type = com.act.maxc.flume.interceptors.DropAvroRecordNullValueInterceptor$build<p>
 *   agent.sources.r1.interceptors.i1.nonNullFields = id,time<p>
 * </code>
 *
 */
public class DropAvroRecordNullValueInterceptor implements Interceptor {

  private static final Logger logger = LoggerFactory
          .getLogger(DropAvroRecordNullValueInterceptor.class);


  
  private String nonNullFields;
  
  /**
   * Only {@link DropAvroRecordNullValueInterceptor.Builder} can build me
   */
  private DropAvroRecordNullValueInterceptor(String nonNullFields) {
	  this.nonNullFields = nonNullFields;
  }

  @Override
  public void initialize() {
    // no-op
  }

  /**
   * Modifies events in-place.
   */
  @Override
  public Event intercept(Event event) {
    Map<String, String> headers = event.getHeaders();
    
    String subject = headers.get("topic");
    String serverUrl = headers.get("serverUrl");
    
    
    String nonNullFieldHeader = headers.get("nonNullField");
    if(StringUtils.isNotBlank(nonNullFieldHeader)) {
    	nonNullFields = nonNullFieldHeader;
    }
    
    
    if(StringUtils.isNotBlank(nonNullFields)) {//如果nullField不为空
    	String[] nonNullFieldSpli = nonNullFields.split(",");
        
        String schemaStr = event.getHeaders().get("schema");
        Schema schema;
        if(StringUtils.isNotBlank(schemaStr)) {
        	schema = new Schema.Parser().parse(schemaStr);
        }else {
        	schema = SchemaRegistryServerUtils.getSchema(serverUrl, subject);
        }
        InputStream inputStream = new ByteArrayInputStream(event.getBody());
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord datum;
        try {
    		while (!decoder.isEnd()) {
    		        datum = reader.read(null, decoder);
    		        
    		        boolean join = judgeWetherNull(nonNullFieldSpli, datum, schema);
    		        
    		        if(!join) {//如果存在nullField且nullField为null值，就抛掉
    		        	return null;
    		        }
    		}
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    }
    return event;
  }

  
  public boolean judgeWetherNull(String[] nonNullFieldSpli, GenericRecord datum, Schema schema) {
	  boolean equal = true;
	  //如果schema包含该字段，且字段值为null，就删除掉
	  for(String nonNullFieldName : nonNullFieldSpli) {
		  if(schema.getField(nonNullFieldName)!=null && datum.get(nonNullFieldName) == null) {
			  logger.info("Dropping record : [" + datum.toString() + "], because the value of its Field : [" + nonNullFieldName + "] is null");
			  equal = false;
			  break;
		  }
	  }
	  return equal;
  }
  
  
  /**
   * Delegates to {@link #intercept(Event)} in a loop.
   * @param events
   * @return
   */
  @Override
  public List<Event> intercept(List<Event> events) {
    for (Event event : events) {
      intercept(event);
    }
    return events;
  }

  @Override
  public void close() {
    // no-op
  }

  /**
   * Builder which builds new instances of the AvroNullValueInterceptor.
   */
  public static class Builder implements Interceptor.Builder {

    private String nonNullFields;

    @Override
    public Interceptor build() {
      return new DropAvroRecordNullValueInterceptor(nonNullFields);
    }

    @Override
    public void configure(Context context) {
      this.nonNullFields = context.getString(nonNullFields);
    }

  }

  public static class Constants {
    public static String NON_NULL_FIELDS = "nonNullFields";

  }

}

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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Simple Interceptor class that sets the host name or IP on all events
 * that are intercepted.<p>
 * The host header is named <code>host</code> and its format is either the FQDN
 * or IP of the host on which this interceptor is run.
 *
 *
 * Properties:<p>
 *
 *   preserveExisting: Whether to preserve an existing value for 'host'
 *                     (default is false)<p>
 *
 *   useIP: Whether to use IP address or fully-qualified hostname for 'host'
 *          header value (default is true)<p>
 *
 *  hostHeader: Specify the key to be used in the event header map for the
 *          host name. (default is "host") <p>
 *
 * Sample config:<p>
 *
 * <code>
 *   agent.sources.r1.channels = c1<p>
 *   agent.sources.r1.type = SEQ<p>
 *   agent.sources.r1.interceptors = i1<p>
 *   agent.sources.r1.interceptors.i1.type = host<p>
 *   agent.sources.r1.interceptors.i1.preserveExisting = true<p>
 *   agent.sources.r1.interceptors.i1.useIP = false<p>
 *   agent.sources.r1.interceptors.i1.hostHeader = hostname<p>
 * </code>
 *
 */
public class AvroObj2DelimitedLineInterceptor implements Interceptor {

  private static final Logger logger = LoggerFactory
          .getLogger(AvroObj2DelimitedLineInterceptor.class);
	private final String newDelimiter;
	private final Schema schema;
	private List<String> fieldNameList = new ArrayList<String>();
	private DatumReader<GenericRecord> reader;
  /**
   * Only {@link AvroObj2DelimitedLineInterceptor.Builder} can build me
   */
  private AvroObj2DelimitedLineInterceptor(Schema schema, String newDelimiter) {
    this.newDelimiter = newDelimiter;
    this.schema = schema;

  }

  @Override
  public void initialize() {
	  List<Field>  fieldList = schema.getFields();
	  for(Field fd : fieldList) {
		  fieldNameList.add(fd.name());
	  }
	  
	 reader = new GenericDatumReader<GenericRecord>(schema);
  }

  /**
   * Modifies events in-place.
   */
  @Override
	public Event intercept(Event event) {

		GenericRecord datum;
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(event.getBody(), null);

		String newBody = "";
		try {
			
			while (!decoder.isEnd()) {
				datum = reader.read(null, decoder);
				StringBuffer sb = new StringBuffer();
				for (String col : fieldNameList) {
					Object colObj = datum.get(col);

					if (colObj != null) {
						sb.append(colObj.toString().trim()).append(newDelimiter);
					} else {
						sb.append(newDelimiter);
					}

				}

				String newSingleBody = sb.toString();
				newSingleBody = newSingleBody.substring(0, newSingleBody.lastIndexOf(newDelimiter)) + "\r\n";
				newBody += newSingleBody;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		 newBody = newBody.substring(0, newBody.lastIndexOf("\r\n"));
		event.setBody(newBody.getBytes());
		if(StringUtils.isNotBlank(newBody)) {
			return event;
		}else {
			return null;
		}
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
   * Builder which builds new instances of the HostInterceptor.
   */
  public static class Builder implements Interceptor.Builder {

    private String newDelimiter = "\u0001";
    private Schema schema;
    

    @Override
    public Interceptor build() {
      return new AvroObj2DelimitedLineInterceptor(schema, newDelimiter);
    }

    @Override
    public void configure(Context context) {
    	newDelimiter = context.getString("newDelimiter", "\u0001");
    	String schemaFilePath =  context.getString("schemaFilePath");
        Preconditions.checkState(schemaFilePath != null, "If using AvroObj2DelimitedLineInterceptor, Configuration must specify a [schemaFilePath]");
    	
        try {
			InputStream in = new FileInputStream(new File(schemaFilePath));
			
			schema = new Schema.Parser().parse(in);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        Preconditions.checkState(schema != null, "Cannot parse file [" + schemaFilePath  + "] as a schema");
        
        
    }

  }



}

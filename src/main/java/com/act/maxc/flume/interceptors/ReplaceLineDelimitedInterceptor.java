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

import java.io.UnsupportedEncodingException;
import java.util.List;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ReplaceLineDelimitedInterceptor implements Interceptor {

  private static final Logger logger = LoggerFactory
          .getLogger(ReplaceLineDelimitedInterceptor.class);
	private final String originalDelimiter;
	private final String newDelimiter;

  /**
   * Only {@link ReplaceLineDelimitedInterceptor.Builder} can build me
   */
  private ReplaceLineDelimitedInterceptor(String originalDelimiter, String newDelimiter) {
    this.originalDelimiter = originalDelimiter;
    this.newDelimiter = newDelimiter;

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
    
    String oldBody = "";
	try {
		oldBody = new String(event.getBody(), "UTF-8");
	} catch (UnsupportedEncodingException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    String[] oldBodySpli = oldBody.split(originalDelimiter);
    StringBuffer sb = new StringBuffer();
    for(String col : oldBodySpli) {
    	sb.append(col).append(newDelimiter);
    }

    String newBody = sb.toString();
    newBody = newBody.substring(0, newBody.lastIndexOf(newDelimiter)-1);
    
    event.setBody(newBody.getBytes());
    
    return event;
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

    private String originalDelimiter = "\t";
    private String newDelimiter = "\u0001";

    @Override
    public Interceptor build() {
      return new ReplaceLineDelimitedInterceptor(originalDelimiter, newDelimiter);
    }

    @Override
    public void configure(Context context) {
    	originalDelimiter = context.getString("originalDelimiter", "\t");
    	newDelimiter = context.getString("newDelimiter", "\u0001");
    }

  }


}

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

package com.act.maxc.flume.sources.file.deserializers;

import com.act.maxc.flume.utils.JsonAvroUtils;
import com.act.maxc.flume.utils.SchemaRegistryServerUtils;
import com.google.common.collect.Lists;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A deserializer that parses text lines from a file.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SchemaFileCsvDeserializer implements EventDeserializer {

	private static final Logger logger = LoggerFactory.getLogger(SchemaFileCsvDeserializer.class);

	private final ResettableInputStream in;
	private final Charset outputCharset;
	private final int maxLineLength;
	private volatile boolean isOpen;
	// 获取schema
	private String schemaFilePath;
	private Schema schema;
	private String topic;

	// 行分隔符
	private String splitRegex;

	public static final String OUT_CHARSET_KEY = "outputCharset";
	public static final String CHARSET_DFLT = "UTF-8";

	public static final String MAXLINE_KEY = "maxLineLength";
	public static final int MAXLINE_DFLT = 2048;

	public static final String SCHEMA_FILE_PATH = "deserializer.schemaFilePath";
	public static final String SPLIT_REGEX = "deserializer.splitRegex";

	SchemaFileCsvDeserializer(Context context, ResettableInputStream in) {
		this.in = in;
		this.outputCharset = Charset.forName(context.getString(OUT_CHARSET_KEY, CHARSET_DFLT));
		this.maxLineLength = context.getInteger(MAXLINE_KEY, MAXLINE_DFLT);
		//自定义属性
		this.schemaFilePath = context.getString(SCHEMA_FILE_PATH);
		this.topic = context.getString("topic");
		File schemaFile = new File(schemaFilePath);
		Schema.Parser parser = new Schema.Parser();
		try {
			this.schema = parser.parse(schemaFile);
			logger.info("schemaFile = " + schema.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.splitRegex = context.getString(SPLIT_REGEX, ",");
		this.isOpen = true;
	}

	/**
	 * Reads a line from a file and returns an event
	 * 
	 * @return Event containing parsed line
	 * @throws IOException
	 */
	@Override
	public Event readEvent() throws IOException {
		ensureOpen();

		Map<String, String> headers = new HashMap<String, String>();

		GenericDatumWriter<GenericRecord> writer = null;
		ByteArrayOutputStream output = null;
		Encoder encoder = null;

		output = new ByteArrayOutputStream();
		writer = new GenericDatumWriter<GenericRecord>(schema);

		encoder = EncoderFactory.get().binaryEncoder(output, null);

		String line = readLine();
		List<Field> fieldList = schema.getFields();
		headers.put("schema", schema.toString());
		headers.put("schemaFilePath", schemaFilePath);
		
		if(line == null ) {
			// 打印日志，说明某行解析错误，并统计
			return null;
		}
		// System.out.println(lineStr);
		String[] lineSpli = line.split(splitRegex);
		// 解析字符相等才拆分序列化字符
		if (lineSpli != null && lineSpli.length == fieldList.size()) {
			GenericRecord datum = new GenericData.Record(schema);
			JsonAvroUtils.putCsvDataIntoGenericRecord(datum, lineSpli, fieldList);
			// System.out.println(datum);
			writer.write(datum, encoder);
			encoder.flush();
			Event event = EventBuilder.withBody(output.toByteArray(), headers);
			output.reset();
			return event;
		} else {
			// 打印日志，说明某行解析错误，并统计
			return null;
		}
	}

	/**
	 * Batch line read
	 * 
	 * @param numEvents
	 *            Maximum number of events to return.
	 * @return List of events containing read lines
	 * @throws IOException
	 */
	@Override
	public List<Event> readEvents(int numEvents) throws IOException {
		ensureOpen();
		List<Event> events = Lists.newLinkedList();
		for (int i = 0; i < numEvents; i++) {
			Event event = readEvent();
			if (event != null) {
				events.add(event);
			} else {
				break;
			}
		}
		return events;
	}

	@Override
	public void mark() throws IOException {
		ensureOpen();
		in.mark();
	}

	@Override
	public void reset() throws IOException {
		ensureOpen();
		in.reset();
	}

	@Override
	public void close() throws IOException {
		if (isOpen) {
			reset();
			in.close();
			isOpen = false;
		}
	}

	private void ensureOpen() {
		if (!isOpen) {
			throw new IllegalStateException("Serializer has been closed");
		}
	}

	// TODO: consider not returning a final character that is a high surrogate
	// when truncating
	private String readLine() throws IOException {
		StringBuilder sb = new StringBuilder();
		int c;
		int readChars = 0;
		while ((c = in.readChar()) != -1) {
			readChars++;

			// FIXME: support \r\n
			if (c == '\n') {
				break;
			}

			sb.append((char) c);

			if (readChars >= maxLineLength) {
				logger.warn("Line length exceeds max ({}), truncating line!", maxLineLength);
				break;
			}
		}

		if (readChars > 0) {
			return sb.toString();
		} else {
			return null;
		}
	}

	public static class Builder implements EventDeserializer.Builder {

		@Override
		public EventDeserializer build(Context context, ResettableInputStream in) {
			return new SchemaFileCsvDeserializer(context, in);
		}

	}

}

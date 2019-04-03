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

import com.act.maxc.flume.utils.SchemaRegistryServerUtils;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.LengthMeasurable;
import org.apache.flume.serialization.RemoteMarkable;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * A deserializer that parses Avro container files, generating one Flume event
 * per record in the Avro file, and storing binary avro-encoded records in the
 * Flume event body.
 */
public class SchemaRegistryServerAvroEventDeserializer implements EventDeserializer {

	private static final Logger logger = LoggerFactory.getLogger(SchemaRegistryServerAvroEventDeserializer.class);

	private final ResettableInputStream ris;

	private Schema schema;
	private DataFileReader<GenericRecord> fileReader;
	private GenericDatumWriter<GenericRecord> datumWriter;
	private GenericRecord record;
	private ByteArrayOutputStream out;
	private BinaryEncoder encoder;

	
	// 获取schema
	private String schemaRegistryUrl;
	private String topic;
	

	public static final String SCHEMA_REGISTRY_URL = "schemaRegistryUrl";
	

	private SchemaRegistryServerAvroEventDeserializer(Context context, ResettableInputStream ris) {
		this.ris = ris;
		this.schemaRegistryUrl = context.getString(SCHEMA_REGISTRY_URL);
		this.topic = context.getString("topic");
		this.schema = SchemaRegistryServerUtils.getSchema(schemaRegistryUrl, topic);
	}

	private void initialize() throws IOException, NoSuchAlgorithmException {
		SeekableResettableInputBridge in = new SeekableResettableInputBridge(ris);
		long pos = in.tell();
		in.seek(0L);
		fileReader = new DataFileReader<GenericRecord>(in, new GenericDatumReader<GenericRecord>());
		fileReader.sync(pos);

		datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		out = new ByteArrayOutputStream();
		encoder = EncoderFactory.get().binaryEncoder(out, encoder);

	}

	public Event readEvent() throws IOException {
		if (fileReader.hasNext()) {
			record = fileReader.next(record);
			out.reset();
			datumWriter.write(record, encoder);
			encoder.flush();
			// annotate header with 64-bit schema CRC hash in hex
			Event event = EventBuilder.withBody(out.toByteArray());
			event.getHeaders().put("schema", schema.toString());
			event.getHeaders().put("serverUrl", schemaRegistryUrl);
			return event;
		}
		return null;
	}

	@Override
	public List<Event> readEvents(int numEvents) throws IOException {
		List<Event> events = Lists.newArrayList();
		for (int i = 0; i < numEvents && fileReader.hasNext(); i++) {
			Event event = readEvent();
			if (event != null) {
				events.add(event);
			}
		}
		return events;
	}

	@Override
	public void mark() throws IOException {
		long pos = fileReader.previousSync() - DataFileConstants.SYNC_SIZE;
		if (pos < 0)
			pos = 0;
		((RemoteMarkable) ris).markPosition(pos);
	}

	@Override
	public void reset() throws IOException {
		long pos = ((RemoteMarkable) ris).getMarkPosition();
		fileReader.sync(pos);
	}

	@Override
	public void close() throws IOException {
		ris.close();
	}

	public static class Builder implements EventDeserializer.Builder {

		@Override
		public EventDeserializer build(Context context, ResettableInputStream in) {
			if (!(in instanceof RemoteMarkable)) {
				throw new IllegalArgumentException(
						"Cannot use this deserializer " + "without a RemoteMarkable input stream");
			}
			SchemaRegistryServerAvroEventDeserializer deserializer = new SchemaRegistryServerAvroEventDeserializer(
					context, in);
			try {
				deserializer.initialize();
			} catch (Exception e) {
				throw new FlumeException("Cannot instantiate deserializer", e);
			}
			return deserializer;
		}
	}

	private static class SeekableResettableInputBridge implements SeekableInput {
		ResettableInputStream ris;

		public SeekableResettableInputBridge(ResettableInputStream ris) {
			this.ris = ris;
		}

		@Override
		public void seek(long p) throws IOException {
			ris.seek(p);
		}

		@Override
		public long tell() throws IOException {
			return ris.tell();
		}

		@Override
		public long length() throws IOException {
			if (ris instanceof LengthMeasurable) {
				return ((LengthMeasurable) ris).length();
			} else {
				// FIXME: Avro doesn't seem to complain about this,
				// but probably not a great idea...
				return Long.MAX_VALUE;
			}
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			return ris.read(b, off, len);
		}

		@Override
		public void close() throws IOException {
			ris.close();
		}
	}

}

package com.act.maxc.flume.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

public class JsonAvroUtils {

	/**
	 * 转换json输入流为eventList
	 * 
	 * @param json
	 * @param schema
	 * @param headers
	 * @return
	 */
	public static List<Event> jsonInputStreamToAvroEventList(InputStream json, Schema schema,
			Map<String, String> headers) {
		// 返回的list
		List<Event> eventList = new ArrayList<Event>();
		// 输出流
		GenericDatumWriter<GenericRecord> writer = null;
		ByteArrayOutputStream output = null;
		BinaryEncoder encoder = null;
		try {
			// 初始化输入流
			DataInputStream dataInputStream = new DataInputStream(json);
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			Decoder decoder = DecoderFactory.get().jsonDecoder(schema, dataInputStream);
			// 初始化输出流
			writer = new GenericDatumWriter<GenericRecord>(schema);
			output = new ByteArrayOutputStream();
			encoder = EncoderFactory.get().binaryEncoder(output, encoder);
			GenericRecord datum;
			while (true) {
				try {
					datum = reader.read(null, decoder);
//					System.out.println(datum);
					writer.write(datum, encoder);
					encoder.flush();
					Event event = EventBuilder.withBody(output.toByteArray(), headers);
					eventList.add(event);
					output.reset();
				} catch (EOFException eofe) {
					break;
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return eventList;
	}

	/**
	 * json byte[] 序列化为压缩的avro
	 * 
	 * @param json
	 * @param schema
	 * @return
	 * @throws IOException
	 */
	public static byte[] jsonToAvro(InputStream json, Schema schema) {
		GenericDatumWriter<GenericRecord> writer = null;
		ByteArrayOutputStream output = null;

		Encoder encoder = null;
		try {
			DataInputStream dataInputStream = new DataInputStream(json);
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			Decoder decoder = DecoderFactory.get().jsonDecoder(schema, dataInputStream);

			output = new ByteArrayOutputStream();
			writer = new GenericDatumWriter<GenericRecord>(schema);

			encoder = EncoderFactory.get().binaryEncoder(output, null);

			GenericRecord datum;
			while (true) {
				try {
					datum = reader.read(null, decoder);
					System.out.println(datum);
				} catch (EOFException eofe) {
					break;
				}
				writer.write(datum, encoder);
			}
			encoder.flush();

			return output.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
			return output.toByteArray();
		}
	}

	/**
	 * 转换avro输入流为eventList
	 * 
	 * @param avro
	 * @param schema
	 * @param headers
	 * @return
	 */
	public static List<Event> avroInputStreamToAvroEventList(InputStream avro, Schema schema,
			Map<String, String> headers) {
		// 返回的list
		List<Event> eventList = new ArrayList<Event>();
		// 输出流
		GenericDatumWriter<GenericRecord> writer = null;
		ByteArrayOutputStream output = null;
		BinaryEncoder encoder = null;
		try {
			// 初始化输入流
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avro, null);
			// 初始化输出流
			writer = new GenericDatumWriter<GenericRecord>(schema);
			output = new ByteArrayOutputStream();
			encoder = EncoderFactory.get().binaryEncoder(output, encoder);

			GenericRecord datum;
			while (!decoder.isEnd()) {
					datum = reader.read(null, decoder);
					System.out.println(datum);
					writer.write(datum, encoder);
					encoder.flush();
					Event event = EventBuilder.withBody(output.toByteArray(), headers);
					eventList.add(event);
					output.reset();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return eventList;
	}

	/**
	 * avro byte[] 序列化为压缩的avro
	 * 
	 * @param json
	 * @param schema
	 * @return
	 * @throws IOException
	 */
	public static byte[] avroToAvro(InputStream avro, Schema schema) {
		GenericDatumWriter<GenericRecord> writer = null;
		ByteArrayOutputStream output = null;
		BinaryEncoder encoder = null;
		try {
			DataInputStream dataInputStream = new DataInputStream(avro);
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(dataInputStream, null);

			output = new ByteArrayOutputStream();
			writer = new GenericDatumWriter<GenericRecord>(schema);

			encoder = EncoderFactory.get().binaryEncoder(output, encoder);

			GenericRecord datum;
			while (!decoder.isEnd()) {
				try {
					datum = reader.read(null, decoder);
					System.out.println(datum);
				} catch (EOFException eofe) {
					break;
				}
				writer.write(datum, encoder);
			}
			encoder.flush();

			return output.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
			return output.toByteArray();
		}
	}

	/**
	 * 转换csv输入流为eventList
	 * 
	 * @param csv
	 * @param splitRegex
	 * @param schema
	 * @param headers
	 * @return
	 */
	public static List<Event> csvInputStreamToAvroEventList(InputStream csv, String splitRegex, Schema schema,
			Map<String, String> headers) {
		// 返回的list
		List<Event> eventList = new ArrayList<Event>();
		// 输出流
		GenericDatumWriter<GenericRecord> writer = null;
		ByteArrayOutputStream output = null;
		BinaryEncoder encoder = null;

		List<Field> fieldList = schema.getFields();
		try {
			// 初始化输入流
			BufferedReader br = new BufferedReader(new InputStreamReader(csv));
			// 初始化输出流
			writer = new GenericDatumWriter<GenericRecord>(schema);
			output = new ByteArrayOutputStream();
			encoder = EncoderFactory.get().binaryEncoder(output, encoder);

			String lineStr;
			while ((lineStr = br.readLine()) != null) {
//				System.out.println(lineStr);
				String[] lineSpli = lineStr.split(splitRegex);
				// 解析字符相等才拆分序列化字符
				if (lineSpli.length == fieldList.size()) {
					GenericRecord datum = new GenericData.Record(schema);
					putCsvDataIntoGenericRecord(datum, lineSpli, fieldList);
//					System.out.println(datum);
					writer.write(datum, encoder);
					encoder.flush();
					Event event = EventBuilder.withBody(output.toByteArray(), headers);
					eventList.add(event);
					output.reset();
				} else {
					// 打印日志，说明某行解析错误，并统计

				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return eventList;
	}

	/**
	 * csv byte[] 序列化为压缩的avro
	 * 
	 * @param csv
	 * @param splitRegex
	 * @param schema
	 * @return
	 */
	public static byte[] csvToAvro(InputStream csv, String splitRegex, Schema schema) {
		GenericDatumWriter<GenericRecord> writer = null;
		ByteArrayOutputStream output = null;
		Encoder encoder = null;

		List<Field> fieldList = schema.getFields();

		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(csv));
			output = new ByteArrayOutputStream();
			writer = new GenericDatumWriter<GenericRecord>(schema);

			encoder = EncoderFactory.get().binaryEncoder(output, null);

			// 启动读数据。先进循环里
			String lineStr;

			while ((lineStr = br.readLine()) != null) {
				System.out.println(lineStr);
				String[] lineSpli = lineStr.split(splitRegex);
				// 解析字符相等才拆分序列化字符
				if (lineSpli.length == fieldList.size()) {
					GenericRecord datum = new GenericData.Record(schema);
					putCsvDataIntoGenericRecord(datum, lineSpli, fieldList);
					System.out.println(datum);
					writer.write(datum, encoder);
				} else {
					// 打印日志，说明某行解析错误，并统计

				}
			}

			encoder.flush();

			return output.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
			return output.toByteArray();
		}
	}

	public static void putCsvDataIntoGenericRecord(GenericRecord datum, String[] lineSpli, List<Field> fieldList) {
		for (int i = 0; i < fieldList.size(); i++) {
			Schema fieldSchema = fieldList.get(i).schema();
			if (fieldSchema != null) {
				FieldTypeEnum fieldTypeEnum = FieldTypeEnum.valueOf(fieldSchema.getType().toString().toUpperCase());
				switch (fieldTypeEnum) {
				case NULL:
					datum.put(fieldList.get(i).name(), lineSpli[i]);
					break;
				case BOOLEAN:
					datum.put(fieldList.get(i).name(), Boolean.parseBoolean(lineSpli[i]));
					break;
				case INT:
					datum.put(fieldList.get(i).name(), Integer.parseInt(lineSpli[i]));
					break;
				case LONG:
					datum.put(fieldList.get(i).name(), Long.parseLong(lineSpli[i]));
					break;
				case FLOAT:
					datum.put(fieldList.get(i).name(), Float.parseFloat(lineSpli[i]));
					break;
				case DOUBLE:
					datum.put(fieldList.get(i).name(), Double.parseDouble(lineSpli[i]));
					break;
				case BYTES:
					datum.put(fieldList.get(i).name(), lineSpli[i].getBytes());
					break;
				case STRING:
					datum.put(fieldList.get(i).name(), lineSpli[i]);
					break;
				default:
					datum.put(fieldList.get(i).name(), lineSpli[i]);
					// System.out.println("Type is incorrect, please check type: " + fieldSchema);
				}
			}

//			System.out.println(fieldSchema.getType());
		}
	}

	/**
	 * Enum for field type
	 *
	 */
	public enum FieldTypeEnum {
		NULL("null"), BOOLEAN("boolean"), INT("int"), LONG("long"), FLOAT("float"), DOUBLE("double"), BYTES(
				"bytes"), STRING("string"), MAP("map"), ARRAY("array"), RECORD("record");

		private String fieldType;

		FieldTypeEnum(String fieldType) {
			this.fieldType = fieldType;
		}

		@Override
		public String toString() {
			return fieldType;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String schemaStr = "{\"type\":\"record\",\"name\":\"testKafka\",\"fields\":[{\"name\":\"id\",\"type\":\"double\"},{\"name\":\"amount\",\"type\":\"double\"}]}";

		Schema schema = new Schema.Parser().parse(schemaStr);
		String jsonStr = "{\"ids\" : \"aaa\", \"amount\" : 100 }{\"ids\" : \"bbb\", \"amount\" : 101 }";

		String csv = "1,101\n" + "2,102\n" + "3,103\n" + "4,104";

		InputStream is = new ByteArrayInputStream(jsonStr.getBytes());

		BufferedReader br = new BufferedReader(new InputStreamReader(is));

//		List<Event> evtList = jsonInputStreamToAvroEventList(is, schema, new HashMap<String, String>());
		
		
        File file = new File("test2.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        try {
			dataFileWriter.create(schema, file);
			GenericRecord test2;
			for(int i =0; i < 1000000; i ++) {
				test2 = new GenericData.Record(schema);
				test2.put("id", i + 0.1);
				test2.put("amount", i + 6.1);
				dataFileWriter.append(test2);
			}
//			System.out.println(test2);
			dataFileWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

}

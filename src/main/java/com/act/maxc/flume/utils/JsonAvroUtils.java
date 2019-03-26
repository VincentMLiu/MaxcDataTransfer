package com.act.maxc.flume.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.output.ByteArrayOutputStream;

public class JsonAvroUtils {

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
	        
	        encoder =  EncoderFactory.get().binaryEncoder(output, null);

	        GenericRecord datum;
	        while (true) {
	            try {
	                datum = reader.read(null, decoder);
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
	    Encoder encoder = null;
	    try {
	    	DataInputStream dataInputStream = new DataInputStream(avro);
	        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
	        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(dataInputStream, null);
	        
	        output = new ByteArrayOutputStream();
	        writer = new GenericDatumWriter<GenericRecord>(schema);
	        
	        encoder =  EncoderFactory.get().binaryEncoder(output, null);

	        GenericRecord datum;
	        while (decoder.isEnd()) {
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
	    	
	    	encoder =  EncoderFactory.get().binaryEncoder(output, null);
	    	while((br.read())!= -1) {
	    		String lineStr = br.readLine();
	    		String[] lineSpli = lineStr.split(splitRegex);
	    		//解析字符相等才拆分序列化字符
	    		if(lineSpli.length == fieldList.size()) {
	    			GenericRecord datum = new GenericData.Record(schema);
	    			for(int i = 0 ; i <  fieldList.size() ; i++) {
	    				datum.put(fieldList.get(i).getProp("name"), lineSpli[i]);
	    			}
	    			System.out.println(datum);
	    			writer.write(datum, encoder);
	    		}else {
	    		//打印日志，说明某行解析错误，并统计
	    		
	    		}
	    	}
	    	
	    	encoder.flush();
	        
	        return output.toByteArray();
	    } catch (IOException e) {
			e.printStackTrace();
	        return output.toByteArray();
		}
	}
	
	
	
	
	public static void main(String[] args) {
		  
		  String schemaStr = "{\"type\":\"record\",\"name\":\"Payment\",\"fields\":[{\"name\":\"ids\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}";
		  
		  Schema schema = new Schema.Parser().parse(schemaStr);
		  String jsonStr = "{\"ids\" : \"aaa\", \"amount\" : 100 }{\"ids\" : \"bbb\", \"amount\" : 101 }";
		  
		  InputStream input = null;
		  input = new ByteArrayInputStream(jsonStr.getBytes());
		  byte[]opt = jsonToAvro(input, schema);
		  
		  InputStream input2 = null;
		  input2 = new ByteArrayInputStream(opt);
		  
		  avroToAvro(input2, schema);
//		  GenericDatumWriter<GenericRecord> writer = null;
//		    Encoder encoder = null;
//		    ByteArrayOutputStream output = null;
//		    try {
////		        Schema schema = new Schema.Parser().parse(schemaStr);
//		        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
//		        
//		        
//		        input = new ByteArrayInputStream(jsonStr.getBytes());
//		        output = new ByteArrayOutputStream();
//		        DataInputStream din = new DataInputStream(input);
//		        
//		        writer = new GenericDatumWriter<GenericRecord>(schema);
//		        encoder =  EncoderFactory.get().binaryEncoder(output, null);
//		        
//		        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
//		        GenericRecord datum;
//		        while (true) {
//		            try {
//		                datum = reader.read(null, decoder);
//		            } catch (EOFException eofe) {
//		                break;
//		            }
//		            writer.write(datum, encoder);
//		        }
//		        
//		        encoder.flush();
//		        byte[] opt = output.toByteArray();
//		        System.out.println(opt);
//		        
//		        BinaryDecoder dataDecoder = new DecoderFactory().binaryDecoder(opt, null);
//		        while (!dataDecoder.isEnd()) {
//		        	GenericRecord datum2 = (GenericRecord)reader.read(null, dataDecoder);
//		        	System.out.println(datum2);
//		        }
//		        
//		    } catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} finally {
//		        try { input.close(); } catch (Exception e) { }
//		    }
		  
	}

}

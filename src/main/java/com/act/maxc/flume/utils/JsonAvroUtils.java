package com.act.maxc.flume.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
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
		} finally {
	        try { json.close(); } catch (Exception e) { }

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
	        Decoder decoder = DecoderFactory.get().binaryDecoder(dataInputStream, null);
	        
	        output = new ByteArrayOutputStream();
	        writer = new GenericDatumWriter<GenericRecord>(schema);
	        
	        encoder =  EncoderFactory.get().binaryEncoder(output, null);

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
		} finally {
	        try { avro.close(); } catch (Exception e) { }

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

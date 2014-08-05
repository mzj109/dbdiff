package com.dbdiff;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class DiffMapper extends
		Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<String>, AvroValue<GenericRecord>> {
	private String pkcol; 
	Schema s1 = Schema.create(Schema.Type.STRING);
	
	enum MyCounters { avrorows }
	
	@Override
	public void setup(Context context) {
		pkcol = context.getConfiguration().get("pkcol");
		
	}


	@Override
	public void map(AvroKey<GenericRecord> key, NullWritable value,
			Context context) throws IOException, InterruptedException {
		
		GenericRecord g = key.datum();
		Schema x = g.getSchema();
		
		context.getCounter(MyCounters.avrorows).increment(1);
		
		String pKey = null;

		for (Field t : x.getFields()) {

  			    System.out.println(t.name()+ g.get(t.name()));
  			    
  			    if (t.name().equals(pkcol)) {
  			    	
  			    	pKey = (String) g.get(t.name());
  			    }
		}
		context.write( new AvroKey<String> (pKey) ,  new AvroValue<GenericRecord> (key.datum()) );

	}



}

package com.dbdiff;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DiffReducer
  extends Reducer<AvroKey<String>, AvroValue<GenericRecord>, Text, Text> {
  

  @Override
  public void reduce(AvroKey<String> key, Iterable<AvroValue<GenericRecord>> values,  Context context) throws IOException, InterruptedException {
	  
	System.out.println(values.toString());
	
	// ultimately these will come from metadata.
	String tL = "t1";
	String tR = "t2";
	String diffString = "";
	Map<String, String> mLVals = new HashMap<String, String>();
	Map<String, String> mRVals = new HashMap<String, String>();
	
    int i = 0;
    for ( AvroValue<GenericRecord> t : values ) {
    	
		GenericRecord g = t.datum();
		Schema x = g.getSchema();
		
		for (Field f : x.getFields()) {
			
			if (x.getName().equals(tL)) {mLVals.put(f.name(),(String) g.get(f.name().toString())); }
			if (x.getName().equals(tR))  {mRVals.put(f.name(),(String) g.get(f.name().toString()));   }
			
		//	System.out.println( "table: "+x.getName() + " column: "+f.name()+" value: "+ g.get(f.name().toString()));
		//	 context.write(new Text("X"), new Text("table: "+x.getName() + " column: "+f.name()+" value: "+ g.get(f.name().toString())));
		}
		
    }
    
    System.out.println(mLVals.size() + ":" + mRVals.size());
    Iterator<?> itL = mLVals.entrySet().iterator();
    while (itL.hasNext()) {
    	Map.Entry pairs = (Map.Entry)itL.next();
    	
    	if (! pairs.getValue().equals(mRVals.get(pairs.getKey()))) { diffString = "Found Diff in column:" + pairs.getKey() + " Data("+ tL + "," + tR+ "):" + pairs.getValue() + "," + mRVals.get(pairs.getKey());} else { diffString = ""; }

    	if (!diffString.equals("") ) {context.write(new Text(key.toString()), new Text(diffString)); System.out.println(diffString); }
    	
    }

    }
  }

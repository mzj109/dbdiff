package com.dbdiff;

import java.io.File;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RunDiff extends Configured implements Tool {

	Schema s1 = Schema.create(Schema.Type.STRING);
	Schema aSchema;
	Schema bSchema;

@Override
  public int run(String[] args) throws Exception {
    
	Job job = new Job(getConf(), "HelloHadoop");
    job.setJarByClass(getClass());

    FileUtil f = new FileUtil();
    
    f.fullyDelete( FileSystem.getLocal(getConf()),new Path("outdir2"));

	DataFileReader<Void> reader2  =	 new DataFileReader<Void>(new File("t2.part-m-00000.avro"),   new GenericDatumReader<Void>());
	bSchema = reader2.getSchema();
	
	DataFileReader<Void> reader =	 new DataFileReader<Void>(new File("t1.part-m-00000.avro"),   new GenericDatumReader<Void>());
	aSchema = reader.getSchema();
	
	//job.setNumReduceTasks(0);
	
	ArrayList<Schema> schemas = new ArrayList<Schema>(); 
	schemas.add(aSchema);
    schemas.add(bSchema);
	Schema unionSchema = Schema.createUnion(schemas); 
	AvroJob.setInputKeySchema(job, unionSchema); 
  
    AvroJob.setMapOutputKeySchema(job, s1);
    AvroJob.setMapOutputValueSchema(job, unionSchema);
     
     //  FileInputFormat.addInputPath(job, new Path("part-m-00000.avro"));
    MultipleInputs.addInputPath(job, new Path("t2.part-m-00000.avro"),  AvroKeyInputFormat.class, DiffMapper.class);
    MultipleInputs.addInputPath(job, new Path("t1.part-m-00000.avro"),  AvroKeyInputFormat.class, DiffMapper.class);

     
     // thats how the schema looks.
     // {"type":"record","name":"t1","doc":"Sqoop import of t1","fields":[{"name":"col1","type":["string","null"],"columnName":"col1","sqlType":"3"},{"name":"col2","type":["string","null"],"columnName":"col2","sqlType":"12"}],"tableName":"t1"}
    FileOutputFormat.setOutputPath(job, new Path("outdir2"));
    
     //  job.setMapOutputKeyClass(NullWritable.class);
    job.setMapperClass(DiffMapper.class);
    job.setReducerClass(DiffReducer.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  //
//   Run with single param: -Dpkcol=col1
//
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new RunDiff(), args);
    System.exit(exitCode);
  }
}
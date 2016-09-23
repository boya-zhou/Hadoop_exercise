package hw1;





import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Query5 extends Configured implements Tool{

	public static class cusMap 
    extends Mapper<LongWritable, Text, Text, Text>{
	  private Text customersID = new Text();
	  private String fileTag = "C";
	  private Text cusName= new Text();
	  public void map(LongWritable key, Text value, Context context
                 ) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] splits = line.split(",");
		  String cusID = splits[0];
		  String name = splits[1];

		  customersID.set(cusID);
		  cusName.set(fileTag+","+name);
		  context.write(customersID, cusName);
 }
 }
public static class transMap
extends Mapper<LongWritable, Text, Text, Text>{
  private Text customersID = new Text();

  private Text transInfo= new Text();
  private String fileTag = "T";
  public void map(LongWritable key, Text value, Context context
             ) throws IOException, InterruptedException {
	  String line = value.toString();
	  String[] splits = line.split(",");
	  
	  String cusID=splits[1];
	  customersID.set(cusID);
	  
	  transInfo.set(fileTag + "," + "1");
	  context.write(customersID, transInfo);
	  
}
}

public static class joinReducer 
extends Reducer<Text, Text,NullWritable,Text> {


  
  String name;
  NullWritable nw = NullWritable.get();

  public void reduce(Text customersID, Iterable<Text> values , 
              Context context
              ) throws IOException, InterruptedException {
	  int sum=0;

	  for (Text val : values) {
		  if (val.toString().split(",")[0].equals("C")){
			  name = val.toString().split(",")[1];
		  }
		  else if (val.toString().split(",")[0].equals("T")){
			  String transTotal = val.toString().split(",")[1];
			  sum += Float.parseFloat(transTotal);
		  }
	  }
	  
	  Text results = new Text();
	  results.set((name+","+Integer.toString(sum)));
	  context.write(nw,results);
}

	  
}
  

	  
  public static class Map 
  extends Mapper<LongWritable, Text, Text, Text>{
	  String name;
	  String transTotal;
	  NullWritable nw = NullWritable.get();

	  public void map(LongWritable key, Text value, Context context
               ) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] splits = line.split(",");
		  
		  name = splits[0];
		  transTotal= splits[1];
		  
		  Text results = new Text();
		  results.set(transTotal);
		  Text nameF = new Text();
		  nameF.set(name);
		  context.write(nameF, results);
}
}
  
  public static class selectReducer 
  	extends Reducer<Text, Text,Text,Text> {
	  		  int minTrans ;
	  		  Text nameFinal = new Text();

	  protected void setup(Context context) throws java.io.IOException, InterruptedException{
		  minTrans=5000000;
	  }

	  int transTotal;
	  public void reduce(Text name, Iterable<Text> values , 
                  Context context
                  ) throws IOException, InterruptedException {
		    
		  for (Text val : values) {
			  transTotal = Integer.parseInt(val.toString());
			  
			  if (transTotal<minTrans){
				   minTrans=transTotal;
				   nameFinal=name;
			  }
		  }
		  
	}
	  protected void cleanup(Context context) throws java.io.IOException, InterruptedException{
		  context.write(nameFinal,new Text(Integer.toString(minTrans)));
	  }
	  
		  
  }
  
	
	
	


  private static final String OUTPUT_PATH = "intermediate_output43";
	
	@Override
	 public int run(String[] args) throws Exception {
		
		
	  /*
	   * Job 1
	   */
	  Configuration conf = getConf();
	  Job job = new Job(conf, "Job1");
	  job.setJarByClass(Query5.class);
	  
	  Path cusInputPath = new Path(args[0]); 
	  Path transInputPath = new Path(args[1]); 
	  Path outputPath = new Path(args[2]);
	  
	  MultipleInputs.addInputPath(job, cusInputPath,
	            TextInputFormat.class, cusMap.class);
	  MultipleInputs.addInputPath(job, transInputPath,
	            TextInputFormat.class, transMap.class);
	    
	  for (String arg : args) {
		  System.out.println(arg);
	  }
	  job.setReducerClass(joinReducer.class);

	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);

	  
	  job.setOutputFormatClass(TextOutputFormat.class);
	  
	  FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

	  job.waitForCompletion(true);

	  /*
	   * Job 2
	   */
	  
	  Job job2 = new Job(conf, "Job 2");
	  job2.setJarByClass(Query5.class);

	  job2.setMapperClass(Map.class);
	  job2.setReducerClass(selectReducer.class);

	  job2.setOutputKeyClass(Text.class);
	  job2.setOutputValueClass(Text.class);

	  job2.setInputFormatClass(TextInputFormat.class);
	  job2.setOutputFormatClass(TextOutputFormat.class);

	  FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
	  FileOutputFormat.setOutputPath(job2, outputPath);

	  return job2.waitForCompletion(true) ? 0 : 1;
	 }

	 /**
	  * Method Name: main Return type: none Purpose:Read the arguments from
	  * command line and run the Job till completion
	  * 
	  */
	public static void main(String[] args) throws Exception{
	// TODO Auto-generated method stub
		if (args.length != 3) {
		      System.err.println("Usage: query5 ");
		      System.exit(3);
		    }	
  ToolRunner.run(new Configuration(),new Query5(), args);
	}
}




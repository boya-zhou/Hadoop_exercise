package hw1;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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



public class Query4 extends Configured implements Tool{

public static class cusMap 
	    extends Mapper<LongWritable, Text, Text, Text>{
		  private Text customersID = new Text();
		  private Text countryID = new Text();
		  private String fileTag = "C";
	   
		  public void map(LongWritable key, Text value, Context context
	                 ) throws IOException, InterruptedException {
			  String line = value.toString();
			  String[] splits = line.split(",");
			  String cusID = splits[0];
			  String countryName = splits[3];

			  customersID.set(cusID);
			  countryID.set(fileTag+","+"1,"+countryName);
			  context.write(customersID, countryID);
	 }
	 }
  public static class transMap
    extends Mapper<LongWritable, Text, Text, Text>{
	  private Text customersID = new Text();
	  private String cusID;
	  private Text transInfo= new Text();
	  private String fileTag = "T";
	  
	  private String transTotal;
	  public void map(LongWritable key, Text value, Context context
                 ) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] splits = line.split(",");
		  
		  cusID=splits[1];
		  
		  transTotal = splits[2];
		  customersID.set(cusID);
		  
		  transInfo.set(fileTag + "," + transTotal);
		  context.write(customersID, transInfo);
		  
 }
}
  
  public static class joinReducer 
  extends Reducer<Text, Text,NullWritable,Text> {

	  float maxTrans ;
	  float minTrans ;
	  String countryName;
	  float transTotal;
	  
	  public void reduce(Text customersID, Iterable<Text> values , 
                  Context context
                  ) throws IOException, InterruptedException {
		  maxTrans = 10;
		  minTrans = 1000;
		    
		  for (Text val : values) {
			  if (val.toString().split(",")[0].equals("C")){
				  countryName = val.toString().split(",")[2];
			  }
			  else if (val.toString().split(",")[0].equals("T")){
				   transTotal = Float.parseFloat(val.toString().split(",")[1]);
				   if (transTotal<minTrans){
					   minTrans=transTotal;}
				   if (transTotal>maxTrans){
					   maxTrans=transTotal;
				  }
			  }
		  }
		  
		  Text results = new Text();
		  results.set((countryName+","+"1"+","+Float.toString(minTrans)+","+Float.toString(maxTrans)));
		  NullWritable nw = NullWritable.get();
		  context.write(nw,results);
	}

		  
  }
  

	  
  public static class Map 
  extends Mapper<LongWritable, Text, Text, Text>{
	  private Text countryID = new Text();
	  private Text countryInfo = new Text();
	   
	  public void map(LongWritable key, Text value, Context context
               ) throws IOException, InterruptedException {
		  String line = value.toString();
		  System.out.println(line);
		  
		  
		  String[] splits = line.split(",");
		  
		  String countryName = splits[0];
		  String min= splits[2];
		  String max= splits[3];
		  countryID.set(countryName);
		  countryInfo.set("1,"+min+","+max);
		  context.write(countryID, countryInfo);
}
}
  
  public static class selectReducer 
  	extends Reducer<Text, Text,Text,Text> {
	  	float transMin;
	  	float transMax;
	  	float maxTrans;
	  	float minTrans;
	  
	  public void reduce(Text countryID, Iterable<Text> values , 
                  Context context
                  ) throws IOException, InterruptedException {
		  int count=0;
		   
			  maxTrans = 10;
			  minTrans = 1000;
		  for (Text val : values) {
			  
			  count = count+Integer.parseInt(val.toString().split(",")[0]);
			  transMin = Float.parseFloat(val.toString().split(",")[1]);
			  transMax = Float.parseFloat(val.toString().split(",")[2]);
			   if (transMin<minTrans){
				   minTrans=transMin;}
			   if (transMax>maxTrans){
				   maxTrans=transMax;
			  }
			
		  }
		  
		  Text results = new Text();
		  results.set((count+","+Float.toString(transMin)+","+Float.toString(maxTrans)));
		  context.write(countryID,results);
	}

		  
  }
  
	
	
	


private static final String OUTPUT_PATH = "intermediate_output40";
		
	@Override
	 public int run(String[] args) throws Exception {
		
		
	  /*
	   * Job 1
	   */
	  Configuration conf = getConf();
	  Job job = new Job(conf, "Job1");
	  job.setJarByClass(Query4.class);
	  
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
	  job2.setJarByClass(Query4.class);

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
		      System.err.println("Usage: query4 ");
		      System.exit(3);
		    }	
    ToolRunner.run(new Configuration(),new Query4(), args);
	}
}




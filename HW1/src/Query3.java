package hw1;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Query3 {

	 public static class cusMap 
	    extends Mapper<LongWritable, Text, LongWritable, Text>{
		  private LongWritable customersID = new LongWritable();
		  private Text customerInfo = new Text();
		  private String fileTag = "C";
	   
		  public void map(LongWritable key, Text value, Context context
	                 ) throws IOException, InterruptedException {
			  String line = value.toString();
			  String[] splits = line.split(",");
			  int cusID = Integer.parseInt(splits[0]);
			  String name = splits[1];
			  float salary = Float.parseFloat(splits[4]);

			  customersID.set(cusID);
			  customerInfo.set(fileTag+","+name+","+Float.toString(salary));
			  context.write(customersID, customerInfo);
	 }
	 }
  public static class transMap
    extends Mapper<LongWritable, Text, LongWritable, Text>{
	  private LongWritable customersID = new LongWritable();
	  Long cusID;
	  private Text transInfo= new Text();
	  private String fileTag = "T";
	  private String transNum ;
	  private String transTotal;
	  public void map(LongWritable key, Text value, Context context
                 ) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] splits = line.split(",");
		  
		  cusID=Long.parseLong(splits[1]);
		  transNum = splits[3];
		  transTotal = splits[2];
		  customersID.set(cusID);
		  
		  transInfo.set(fileTag + "," + "1," + transTotal+","+transNum);
		  context.write(customersID, transInfo);
		  
 }
}
  
  public static class joinReducer 
  extends Reducer<LongWritable, Text,LongWritable,Text> {
	  String name;
	  String salary;
	  int numTrans ;
	  float sumTotal ;
	  int minnum ;
	  int transNumItem;
	  public void reduce(LongWritable customersID, Iterable<Text> values , 
                  Context context
                  ) throws IOException, InterruptedException {
		  numTrans = 0;
		  sumTotal = 0;  
		  minnum = 10;
		  for (Text val : values) {
			  if (val.toString().split(",")[0].equals("C")){
				  name = val.toString().split(",")[1];
				  salary = val.toString().split(",")[2];
			  }
			  else if (val.toString().split(",")[0].equals("T")){
				  String num = val.toString().split(",")[1];
				  numTrans +=Integer.parseInt(num);
				  String transTotal = val.toString().split(",")[2];
				  sumTotal += Float.parseFloat(transTotal);
				  transNumItem = Integer.parseInt(val.toString().split(",")[3]);
				  if (transNumItem<minnum){
					  minnum = transNumItem;
				  }
			  }
		  }
		  
		  Text results = new Text();
		  results.set((name+","+salary+","+Integer.toString(numTrans)+","+Float.toString(sumTotal)+","+Integer.toString(minnum)));
		  context.write(customersID,results);
	}

		  
  }
	  

  
	public static void main(String[] args) throws Exception{
	// TODO Auto-generated method stub
	Configuration conf = new Configuration();
    if (args.length != 3) {
      System.err.println("Usage: query3 <HDFS input file1> <HDFS input file2> <HDFS output file>");
      System.exit(3);
    }
    Job job = new Job(conf, "query3");
    job.setJarByClass(Query3.class);
    Path cusInputPath = new Path(args[0]); 
    Path transInputPath = new Path(args[1]); 
    Path outputPath = new Path(args[2]);

    job.setReducerClass(joinReducer.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(TextInputFormat.class);

    MultipleInputs.addInputPath(job, cusInputPath,
            TextInputFormat.class, cusMap.class);
    MultipleInputs.addInputPath(job, transInputPath,
            TextInputFormat.class, transMap.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Query2 {

	
  public static class Map 
    extends Mapper<LongWritable, Text, LongWritable, Text>{
//	  private final static IntWritable one = new IntWritable(1);
	  private LongWritable customersID = new LongWritable();
	  private Text transInfo= new Text();
   
	  public void map(LongWritable key, Text value, Context context
                 ) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] splits = line.split(",");
		  customersID.set(Long.parseLong(splits[1]));
		  
		  transInfo.set("1,"+ splits[2]);
		  context.write(customersID, transInfo);
		  
		  
 }
}
  
  public static class transReducer 
  extends Reducer<LongWritable, Text,LongWritable,Text> {
	  
	  public void reduce(LongWritable customersID, Iterable<Text> values , 
                  Context context
                  ) throws IOException, InterruptedException {
		  int sum1 = 0;
		  float sum2 = 0;
		  
		  for (Text val : values) {
			  String num = val.toString().split(",")[0];
			  sum1 +=Integer.parseInt(num);
			  String transTotal = val.toString().split(",")[1];
			  sum2 += Float.parseFloat(transTotal);
		  }
//		  numTransactions.set(sum1);
//		  totalSum.set((long) sum2);
		  Text finalText = new Text();
//		  finalText.set(Integer.toString(sum1));
		  finalText.set(Integer.toString(sum1)+","+Float.toString(sum2));
//		  Text num = new Text();
//		  num.set(Integer.toString(sum1));
		  context.write(customersID, finalText);
	  }
	  
}
  
	public static void main(String[] args) throws Exception{
	// TODO Auto-generated method stub
	Configuration conf = new Configuration();
    if (args.length != 2) {
      System.err.println("Usage: query2 <HDFS input file> <HDFS output file>");
      System.exit(2);
    }
    Job job = new Job(conf, "query2");
    job.setJarByClass(Query2.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(transReducer.class);
    job.setReducerClass(transReducer.class);
//    job.setNumReduceTasks(2);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

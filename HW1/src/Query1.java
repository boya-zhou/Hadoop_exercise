package hw1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Query1 {

	
  public static class TokenizerMapper 
    extends Mapper<LongWritable, Text, IntWritable, Text>{
//	  private final static IntWritable one = new IntWritable(1);
	  private IntWritable countryID = new IntWritable();
	  private Text customerInfo = new Text();
   
	  public void map(LongWritable key, Text value, Context context
                 ) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] splits = line.split(",");
		  int countryCode = Integer.parseInt(splits[3]);
		  if (countryCode>=2 && countryCode<=6){
			  countryID.set(countryCode);
			  customerInfo.set(line);
			  context.write(countryID, customerInfo);
		  }
		  
 }
}
	public static void main(String[] args) throws Exception{
	// TODO Auto-generated method stub
	Configuration conf = new Configuration();
    if (args.length != 2) {
      System.err.println("Usage: query1 <HDFS input file> <HDFS output file>");
      System.exit(2);
    }
    Job job = new Job(conf, "query1");
    job.setJarByClass(Query1.class);
    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
//    job.setReducerClass(IntSumReducer.class);
//    job.setNumReduceTasks(2);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

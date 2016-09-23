package hw4Query3;

import java.io.IOException;  
import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;  
import java.io.FileReader;
import java.io.InputStreamReader;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.LongWritable;

@SuppressWarnings("deprecation")
public class HW4Query3 extends Configured implements Tool{
	
	    public static List<String> centerlist = new ArrayList<>();
	    public static int k = 6;//the number of k 
	    public static String[] points = new String[k]; 
	    private static BufferedReader brReader;
	    // iteration Control, Maximun 6 iterations here
		private static int MAXITERATIONS = 6;
		private static int THRESHOLD ;
		public static String taskid;
		public static int iteration = 0;
		
	    public static class MapClass extends Mapper<LongWritable, Text, LongWritable, Text> {  
	          
	        private Text word = new Text();
	        
	        //read from cache and take the first k nodes, put in centerlist
	        @Override
			protected void setup(Context context) throws IOException,InterruptedException {
				try{
					@SuppressWarnings("deprecation")
					Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
					String stringCenters = null;
					for (Path eachPath : paths) {
						if (eachPath.getName().toString().trim().equals("initial_point_new.txt")) {
							brReader = new BufferedReader(new FileReader(eachPath.toString()));
							while(null !=(stringCenters = brReader.readLine())){
								centerlist.add(stringCenters);
							}
							for (int i=0; i < k ; i ++){
					        	points[i] = centerlist.get(i);
					        }
						}
					}
					}
				catch (IOException e)
				{
				e.printStackTrace();
				}
				finally
				{
					try{
						brReader.close();  
	                }  
	                catch (IOException e)  
	                {  
	                    e.printStackTrace();  
	                }  
	            } 
			}
	        
	        
	        
	        //use map function to calculate the closet distance for each node
	        //the key is the index of centerlist
	        public void map(LongWritable key, Text value,Context context) throws 
	        	IOException,InterruptedException { 
	        
	        	String coordination[];
	        	LongWritable keyCenter = new LongWritable();
	        	int x,y;
	        	int index =0 ;
	        	int xCenter = 0;
	        	int yCenter = 0;

	        	coordination = value.toString().split(",");
	        	x = Integer.parseInt(coordination[0]);
	        	y = Integer.parseInt(coordination[1]);
	        	Float minDistance = 100000f;
	        	Float distance = 0f;
	        	for (int i=0; i < k ; i ++){
	        		xCenter = Integer.parseInt(points[i].split(",")[0]);
	        		yCenter = Integer.parseInt(points[i].split(",")[1]);
	        		distance = (float) Math.sqrt(Math.pow((x - xCenter), 2) + Math.pow((y - yCenter), 2));
	        		if (distance < minDistance){
	        			minDistance = distance;
	        			index = i;
	        		}
	        	}
	        	keyCenter.set(index);
	        	context.write(keyCenter, value);
	        	
	        }  
	    }  
	    
	    public static class Combiner extends Reducer<LongWritable, Text, LongWritable, Text> {
	    	public void reduce(LongWritable key, Iterable<Text> values,  
                    Context context) throws IOException,InterruptedException {
		    	long n = 0;
				long x_sum = 0;
				long y_sum = 0;

		    	for (Text val:values) {
		    		
					String point = val.toString();
	        	    String[] data = point.split(",");
		    		Integer x = Integer.parseInt(data[0]);
					Integer y = Integer.parseInt(data[1]);
					
					x_sum += (long)x;
					y_sum += (long)y;
					
					n = n + 1;
		    	}
		        context.write(key, new Text(x_sum + "," + y_sum + "," + n));
		    }
		}

	    
	    
	     
	    public static class ClusterReduce extends Reducer<LongWritable, Text, Text, Text> {
	    	
	    	Text newCenter = new Text();
	        
	    	
	    	public void reduce(LongWritable key, Iterable<Text> values,  
	                           Context context) throws IOException,InterruptedException {  
	    		long nInfo = 0;
	    		long xInfo = 0;
	    		long yInfo = 0; 
	        	long sumX = 0;
	        	long sumY = 0;
	        	long sumN = 0;
	        	long x = 0;
	        	long y = 0;
	        	long distance = 0;
				
				taskid = String.valueOf(context.getTaskAttemptID().getTaskID().getId());

				for (Text val:values){
					xInfo = Long.parseLong(val.toString().split(",")[0]);
					yInfo = Long.parseLong(val.toString().split(",")[1]);
					nInfo = Long.parseLong(val.toString().split(",")[2]);
					sumN = sumN + nInfo;
					sumX = sumX + xInfo;
					sumY = sumY + yInfo;
				}
				x = sumX/sumN;
				y = sumY/sumN;
				distance = (long) Math.sqrt(Math.pow((x - xInfo), 2) + Math.pow((y - yInfo), 2));
				
				newCenter.set(new Text(x + "," + y));
				
				if (distance < THRESHOLD){
					context.write(newCenter, new Text( "!"));
				}else{
					context.write(newCenter,new Text( "@"));
				}
				
				
	        }  
	    	
	    	
	    }  
	    
	    public static int stopIteration(Configuration conf) throws IOException 
		{
			FileSystem fs = FileSystem.get(conf);
			Path pervCenterFile = new Path("/usr/share/hadoop/hw4/initial_point_new.txt");
			

			Path currentCenterFile = new Path("/usr/share/hadoop/hw4_output3_001/_temporary/"+ taskid +"/part-r-00000");
			if(!(fs.exists(pervCenterFile) && fs.exists(currentCenterFile)))
			{
				System.exit(1);
			}
			
			
			if (iteration !=6){
				fs.delete(pervCenterFile,true);

			}
			if(fs.rename(currentCenterFile, pervCenterFile) == false)
			{
				System.exit(1);
			}
			
			//check whether the centers have changed or not to determine to do iteration or not
			String line;
			FSDataInputStream in = fs.open(pervCenterFile);
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);

			
			while ((line = br.readLine()) != null) {
				if (line.contains("@"))
				{
					try {
						if (br != null){
							br.close();
						}
					} catch (IOException ex) {
						ex.printStackTrace();
					}
					
				}
			}
			
			return 1;
		}
	    
	    
	    @Override
		public int run(String[] args) throws Exception 
		{
			Configuration conf = getConf();
			FileSystem fs = FileSystem.get(conf);
			Job job = new Job(conf);
			job.setJarByClass(HW4Query3.class);
			
			FileInputFormat.setInputPaths(job, "/usr/share/hadoop/hw4/P3_point.txt");
			Path outDir = new Path("/usr/share/hadoop/hw4_output3_002");
			fs.delete(outDir,true);
			FileOutputFormat.setOutputPath(job, outDir);
			 
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setMapperClass(MapClass.class);
			job.setCombinerClass(Combiner.class);
			job.setReducerClass(ClusterReduce.class);
			job.setNumReduceTasks(1);// so that all new centroids will output into one file
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			return job.waitForCompletion(true)?0:1;
		}
		
		
		@SuppressWarnings("deprecation")
		public static void main(String[] args) throws Exception 
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			
			k = Integer.parseInt(args[0]);
			THRESHOLD = (int) Double.parseDouble(args[1]);
			boolean waitingForConverge = false;
			if(MAXITERATIONS == 0)
			{
				waitingForConverge = true;
			}
			
			
			// set the path for cache, which will be loaded in ClusterMapper
			Path dataFile = new Path("/usr/share/hadoop/hw4/initial_point_new.txt");
			DistributedCache.addCacheFile(dataFile.toUri(),conf);
	 
			int success = 1;
			do 
			{
				success ^= ToolRunner.run(conf, new HW4Query3(), args);
				iteration++;
			} while (success == 1 && (stopIteration(conf) == 1) && (waitingForConverge || iteration < MAXITERATIONS) ); // take care of the order, I make stopIteration() prior to iteration, because I must keep the initK always contain the lastest centroids after each iteration
			 
			
			
		}
	        
		 
	}  
	
	


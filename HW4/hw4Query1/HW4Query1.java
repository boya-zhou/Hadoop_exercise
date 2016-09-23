package hw4Query1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HW4Query1 extends Configured implements Tool{
	public static class PointsJoin extends
		Mapper<LongWritable, Text, Text, Text> {
		public static String window;
		
		
		@Override
		public void setup(Context context) throws IOException,
			InterruptedException {
			Configuration conf = context.getConfiguration();
			window = conf.get("window");
			
		}
				
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			Text outKey = new Text();
			Text outValue = new Text();
			String pointsKey = "";
			String pointsValue = "";
			
            String[] points = value.toString().split(",");  
            Integer x = Integer.parseInt(points[0]);  
            Integer y = Integer.parseInt(points[1]);
            int scale = 100;
            
            //get the coordinate of window(down_left and top_right)
            String[] windows = window.split(",");
            Integer x1 = Integer.parseInt(windows[0]);
            Integer y1 = Integer.parseInt(windows[1]);
            Integer x2 = Integer.parseInt(windows[2]);
            Integer y2 = Integer.parseInt(windows[3]);
            // Filter the points that do not in window
            if (x >= x1 && x <=x2 && y<=y1 && y<=y2){
            	float key_x = (float)x/scale;
            	float key_y = (float)y/scale;
            	
            	pointsKey = String.valueOf((int)key_x)+String.valueOf((int)key_y);
            	pointsValue = value.toString();
            	outKey.set(pointsKey);  
                outValue.set(pointsValue);  
                context.write(outKey, outValue);
            }     
        } 
	}
	
	public static class RectangularJoin extends
	Mapper<LongWritable, Text, Text, Text> {
			public static String window;
			private Text outKey = new Text();
			private Text outValue = new Text();			
			private String pointsKey;
			
			
			@Override
			protected void setup(Context context) throws IOException,
				InterruptedException {
				Configuration conf = context.getConfiguration();
				window = conf.get("window");
				
			}
					
			@Override
			public void map(LongWritable key, Text value, Context context) 
					throws IOException, InterruptedException{  
	            String[] points = value.toString().split(",");
	            String rName = points[0];
	            Integer x_left = Integer.parseInt(points[1]);  
	            Integer y_down = Integer.parseInt(points[2]);
	            Integer x_right = Integer.parseInt(points[3]);  
	            Integer y_top = Integer.parseInt(points[4]);
	            int scale = 100;
	            
	            //get the coordinate of window(down_left and top_right)
	            String[] windows = window.split(",");
	            Integer x1 = Integer.parseInt(windows[0]);
	            Integer y1 = Integer.parseInt(windows[1]);
	            Integer x2 = Integer.parseInt(windows[2]);
	            Integer y2 = Integer.parseInt(windows[3]);
	            
	            // Filter the rectangular that do not overlap with window
	            if (!(x_right<x1||x_left>x2 || y_top<y1||y_down>y2)){
	            	// a rectangular can cover several grid
	            	int key_x_left = x_left/scale;
	            	int key_x_right = x_right/scale;
	            	int key_y_down = y_down/scale;
	            	int key_y_top = y_top/scale;
	            	
	            	for (int k = key_x_left; k<= key_x_right;k++){
	            		for (int j = key_y_down; j<= key_y_top;j++){
	    	            	pointsKey = String.valueOf((int)k)+String.valueOf((int)j);
	    	            	outKey.set(pointsKey);  
	    		            outValue.set(rName + "," + x_left + "," +y_down + "," + x_right + "," +y_top);  
	    		            context.write(outKey, outValue); 
	            		}
	            	}	
	            }    
	        } 
}

	public static class Reduce 
  		extends Reducer<Text, Text,NullWritable,Text> {
	  	List<String> points = new ArrayList<>();
	  	List<String> renctangulars = new ArrayList<>();
	  	
	  	Text results = new Text();
		NullWritable nw = NullWritable.get();

	    public void reduce(Text key, Iterable<Text> values , 
                  Context context) throws IOException, InterruptedException {
		  
		  for (Text val : values) {
			  String[] splits = val.toString().split(",");
			  if(splits.length==2){
				  points.add(val.toString());
			  }
			  else{
				  renctangulars.add(val.toString());
			  }
			
		  }
		  
		  for (String rect:renctangulars){
			  String rectSplits[] = rect.split(",");
			  String rname = rectSplits[0];
				
			  float x_left = Float.parseFloat(rectSplits[1]);
			  float y_down = Float.parseFloat(rectSplits[2]);
			  float x_right = Float.parseFloat(rectSplits[3]);
			  float y_top = Float.parseFloat(rectSplits[4]);
			  for(String poi:points){
				  String poiSplits[] = poi.split(",");
				  float x = Float.parseFloat(poiSplits[0]);
				  float y = Float.parseFloat(poiSplits[1]);
				  if((x<x_right&&x>x_left)&&(y<y_top&&y>y_down)){
					  results.set("<" + rname + ",(" + x + "," + y + ")>");
					  context.write(nw,results);
				  }
			  }
		  }	  
	}
}	
	 

	public static void main(String[] args) throws Exception  
    {  
        int res = ToolRunner.run(new Configuration(), new HW4Query1(), args);  
        System.exit(res);  
    }  


	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//set window as parameter
		conf.set("window", "100,45,5000,95");
		
		Job job = new Job(conf);
		job.setJobName("hw4_Query1");  
		job.setJarByClass(HW4Query1.class);
		
  
        
        Path pointsInputPath = new Path(args[0]); 
  	  	Path rectangularInputPath = new Path(args[1]); 
  	  	Path outputPath = new Path(args[2]);
  	  
  	  	MultipleInputs.addInputPath(job, pointsInputPath,
  	            TextInputFormat.class, PointsJoin.class);
  	  	MultipleInputs.addInputPath(job, rectangularInputPath,
  	            TextInputFormat.class, RectangularJoin.class);
        job.setReducerClass(Reduce.class);  

		FileOutputFormat.setOutputPath(job, outputPath); 
  
        job.setMapOutputKeyClass(Text.class);  
        job.setMapOutputValueClass(Text.class);  
          
  	  	job.setOutputKeyClass(Text.class);
  	  	job.setOutputValueClass(Text.class); 
  
        boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;    
	}

	
}

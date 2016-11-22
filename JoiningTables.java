import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoiningTables {

	public static class PageViewMapper
				extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text joinKey = new Text();
		private Text joinValue = new Text();
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
	
			String[] split = value.toString().split("\t");
			if (Character.isLetter(split[0].charAt(0))) {
				for (int i = 0; i < split.length; i++) {
					split[i] = "#" + split[i];
				}
			}
			joinKey.set(split[1]);
			joinValue.set("pv" + "\t" + split[0]);
			context.write(joinKey,joinValue);
		}
	}
	
	public static class UserMapper
				extends Mapper<LongWritable, Text, Text, Text> {
	
		private Text joinKey = new Text();
		private Text joinValue = new Text();
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
	
			String[] split = value.toString().split("\t");
			if (Character.isLetter(split[0].charAt(0))) {
				for (int i = 0; i < split.length; i++) {
					split[i] = "#" + split[i];
				}
			}
			joinKey.set(split[0]);
			joinValue.set("u" + "\t" + split[1]);
			context.write(joinKey,joinValue);
		}
	}
	
	
	public static class JoinReducer
				extends Reducer<Text,Text,Text,Text> {
		
		private Text outKey = new Text();
		private Text outValue = new Text();	
	
	  	public void reduce(Text key, Iterable<Text> values, Context context) 
	  			throws IOException, InterruptedException {
	  		
	  		ArrayList<Text> listPageID = new ArrayList<Text>();
			ArrayList<Text> listAge = new ArrayList<Text>();
	  		
	  		for (Text val : values) {
	  	        String[] valString = val.toString().split("\t");
	  	        if (valString[0].equals("pv")) {
	  	        	if (valString[1].charAt(0)=='#') {
	  	        		valString[1] = valString[1].substring(1);
	  	        	}
	  	        	listPageID.add(new Text(valString[1]));
	  	        } else if (valString[0].equals("u")) {
	  	        	if (valString[1].charAt(0)=='#') {
	  	        		valString[1] = valString[1].substring(1);
	  	        	}
	  	        	listAge.add(new Text(valString[1]));
	  	        }
	  		}
	  		for (Text p: listPageID) {
	  			for (Text a: listAge) {
	  				outKey.set(p.toString());
	  				outValue.set(a.toString());
	  				context.write(outKey, outValue);
	  			}
		  	}
	  	}
	}
	
	public static void main(String[] args) throws Exception {
		
	  Configuration conf = new Configuration();
	  Job job = Job.getInstance(conf, "join tables");
	  job.setJarByClass(JoiningTables.class);
	  
	  MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,PageViewMapper.class);
	  MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,UserMapper.class);
	  
	  job.setReducerClass(JoinReducer.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  
	  FileOutputFormat.setOutputPath(job, new Path(args[2]));
	  System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

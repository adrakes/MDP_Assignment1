package reduced_inverted_index;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RemoveStopWords extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new RemoveStopWords(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "RemoveStopWords");
      job.setJarByClass(RemoveStopWords.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setNumReduceTasks(1);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setCombinerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.getConfiguration().set("mapreduce.output.textoutputformat.seperator", "->");
      

      
      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      private final static Text word = new Text();
      private Text source = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	 HashSet<String> SW = new HashSet<String>();
    	 BufferedReader LineReader = new BufferedReader(
    			 new FileReader(new File("/home/cloudera/workspace/Homework1/output/StopWords_final_1.txt")));
    	 String ord;
    	 while ((ord = LineReader.readLine()) != null) {
    		 SW.add(ord.toLowerCase());
    	 }
    	
    	 String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
    	 source = new Text(filenameStr);
    	 
         for (String token: value.toString().split("\\s+")) {
        	 if (!SW.contains(token.toLowerCase())) {
        		 word.set(token.toLowerCase());
        	 }
         }
            context.write(word, source);
         }
      }
   

   public static class Reduce extends Reducer<Text, Text, Text, Text> {
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	 HashSet<String> set = new HashSet<String>();
    	 
    	 for (Text verdi : values) {
    		 set.add(verdi.toString());
    	 }
    	 
    	 StringBuilder bygger = new StringBuilder();
    	 
    	 String pfx = "";
    	 for (String verdi : set) {
    		 bygger.append(pfx);
    		 pfx = ", ";
    		 bygger.append(verdi);
    	 }
    	 context.write(key, new Text(bygger.toString()));
    	 
        
      }
   }
}

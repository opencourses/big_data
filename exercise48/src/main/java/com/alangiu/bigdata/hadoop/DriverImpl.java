package com.alangiu.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverImpl extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Path inputPath;
    Path outputDir1;
    Path outputDir2;
    int numberOfReducers;
	
    numberOfReducers = Integer.parseInt(args[0]);
    inputPath = new Path(args[1]);
    outputDir1 = new Path(args[2]);
    outputDir2 = new Path(args[3]);
    
    Configuration conf = this.getConf();

    Job job = Job.getInstance(conf); 
    job.setJobName("exercise48");
    
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputDir1);
    
    job.setJarByClass(DriverImpl.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
       
    job.setMapperClass(Mapper1Impl.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setReducerClass(Reducer1Impl.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setNumReduceTasks(numberOfReducers);
    
    
    if (job.waitForCompletion(true)==false) {
    	return 1;
    }
    
    Job job2 = Job.getInstance(conf); 
    job2.setJobName("exercise48");
    
    FileInputFormat.addInputPath(job2, outputDir1);
    FileOutputFormat.setOutputPath(job2, outputDir2);
    
    job2.setJarByClass(DriverImpl.class);
    job2.setInputFormatClass(KeyValueTextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
       
    job2.setMapperClass(Mapper2Impl.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(IntWritable.class);
    
    job2.setReducerClass(Reducer2Impl.class);
    job2.setOutputKeyClass(NullWritable.class);
    job2.setOutputValueClass(Text.class);

    job2.setNumReduceTasks(numberOfReducers);
    if (job2.waitForCompletion(true)==false) {
    		return 1;
    }
    return 0;
  }
  
  public static int main(String args[]) throws Exception {
    return ToolRunner.run(new Configuration(), new DriverImpl(), args);
  }
  
}

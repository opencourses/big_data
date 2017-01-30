package com.alangiu.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverImpl extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Path inputPath1;
    Path inputPath2;
    Path outputDir;
    int numberOfReducers;
	
    numberOfReducers = Integer.parseInt(args[0]);
    inputPath1 = new Path(args[1]);
    inputPath2 = new Path(args[2]);
    outputDir = new Path(args[3]);
    
    Configuration conf = this.getConf();

    Job job = Job.getInstance(conf); 
    job.setJobName("exercise16");
    
    MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, Mapper1Impl.class);
    MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, Mapper2Impl.class);
    FileOutputFormat.setOutputPath(job, outputDir);
    
    job.setJarByClass(DriverImpl.class);
    job.setOutputFormatClass(TextOutputFormat.class);
       
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    
    job.setCombinerClass(CombinerImpl.class);
    
    job.setReducerClass(ReducerImpl.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    job.setNumReduceTasks(numberOfReducers);
    
    
    if (job.waitForCompletion(true)==true) {
    	return 0;
    }
    return 1;
  }
  
  public static int main(String args[]) throws Exception {
    return ToolRunner.run(new Configuration(), new DriverImpl(), args);
  }
  
}

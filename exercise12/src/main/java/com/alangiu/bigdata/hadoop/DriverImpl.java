package com.alangiu.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverImpl extends Configured implements Tool {

	public static final String THRESHOLD = "THRESHOLD";

	@Override
	public int run(String[] args) throws Exception {

		Path inputPath;
		Path outputDir;

		inputPath = new Path(args[0]);
		outputDir = new Path(args[1]);
		double threshold = Double.parseDouble(args[2]);

		Configuration conf = this.getConf();

		conf.setDouble(THRESHOLD, threshold);

		Job job = Job.getInstance(conf);
		job.setJobName("exercise12");

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.setJarByClass(DriverImpl.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(MapperImpl.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		if (job.waitForCompletion(true) == true) {
			return 0;
		}
		return 1;
	}

	public static int main(String args[]) throws Exception {
		return ToolRunner.run(new Configuration(), new DriverImpl(), args);
	}

}

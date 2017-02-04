package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class Reducer2Impl extends Reducer<Text, IntWritable, NullWritable, Text> {

	private int max = 0;
	private Text maxKey;

	// This should reduce to a single max value all the max received
	// from all the reducers
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// The iterable should contain just one value;
		int local_max = values.iterator().next().get();
		if (local_max > max) {
			max = local_max;
			maxKey = key;
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), maxKey);
	}

}

package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerImpl extends Reducer<LongWritable, Text,
										  Text, NullWritable> {
	

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		// the element should be always in the first element of the iterator
		context.write(values.iterator().next(), NullWritable.get());
	}
	

}

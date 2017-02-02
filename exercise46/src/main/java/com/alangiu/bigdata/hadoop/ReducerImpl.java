package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerImpl extends Reducer<Text, DoubleWritable, Text, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		Double sum = 0.0;
		for (DoubleWritable d : values ){
			sum+= d.get();
		}
		if (sum > 100) {
			context.write(key, NullWritable.get());
		}
	}
}

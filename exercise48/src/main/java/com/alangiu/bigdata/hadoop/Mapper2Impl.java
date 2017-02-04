package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class Mapper2Impl extends Mapper<Text, Text, Text, IntWritable> {

	private int top = -1;
	private Text topKey;

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		Integer a = Integer.parseInt(value.toString());
		if (a > top) {
			top = a;
			topKey = new Text(key.toString());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(topKey, new IntWritable(top));
	}

}

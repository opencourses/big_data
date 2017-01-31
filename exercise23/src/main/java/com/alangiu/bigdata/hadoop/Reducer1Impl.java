package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1Impl extends Reducer<Text, Text, Text, NullWritable>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		String out=null;
		for (Text t: values) {
			if (out == null) {
				out = t.toString();
				continue;
			}
			out += ","+t.toString();
		}
		context.write(new Text(out), NullWritable.get());
	}
	
}

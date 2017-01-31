package com.alangiu.bigdata.hadoop;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2Impl extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		for (String v: values) {
			for (String t: values) {
				if (!v.equals(t)) {
					context.write(new Text(v), new Text(t));
				}
			}
		}
	}
	
}

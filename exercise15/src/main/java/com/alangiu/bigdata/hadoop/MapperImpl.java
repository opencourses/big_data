package com.alangiu.bigdata.hadoop;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperImpl extends Mapper<LongWritable, Text, Text, NullWritable> {

	private HashSet<String> set = new HashSet<>();

	@Override
	protected void map(LongWritable key, Text value, Context context) {
		String[] values = value.toString().split("\\s+");
		for (String v : values) {
			v=v.trim().replaceAll(",|\\.","");
			if (!set.contains(v)) {
				set.add(v);
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (String s : set) {
			context.write(new Text(s), NullWritable.get());
		}
	}
}

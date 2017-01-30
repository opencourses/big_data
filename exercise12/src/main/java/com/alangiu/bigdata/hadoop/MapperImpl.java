package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperImpl extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		Double d = Double.parseDouble(value.toString());
		if (d < context.getConfiguration().getDouble(DriverImpl.THRESHOLD, 0.0)) {
			context.write(key, value);
		}

	}
}

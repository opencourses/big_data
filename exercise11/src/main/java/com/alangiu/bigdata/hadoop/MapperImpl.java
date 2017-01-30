package com.alangiu.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperImpl extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private HashMap<String, Average> data = new HashMap<>();

	@Override
	protected void map(LongWritable key, Text value, Context context) {
		String[] values = value.toString().split(",");

		Average a = data.get(values[0]);
		if (a == null) {
			a = new Average();
		}
		a.sum += Double.parseDouble(values[2]);
		a.num++;
		data.put(values[0], a);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Entry<String, Average> e : data.entrySet()) {
			context.write(new Text(e.getKey()), new DoubleWritable(e.getValue().calculate()));
		}
	}
}

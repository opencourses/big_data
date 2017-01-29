package com.alangiu.bigdata.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerImpl extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) 
						throws IOException, InterruptedException {
		String dates = "";
		Iterator<Text> i = values.iterator();
		if (i.hasNext()) {
			dates = i.next().toString();
		}
		while (i.hasNext()) {
			dates = dates.concat(", " + i.next().toString());
		}
		context.write(key, new Text(dates));
	}
}

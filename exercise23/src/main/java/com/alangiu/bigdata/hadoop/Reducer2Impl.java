package com.alangiu.bigdata.hadoop;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2Impl extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
								throws IOException, InterruptedException {
		String out = null;
		HashSet<String> set = new HashSet<>();
		
 		for (Text t: values) {
			if (out == null) {
				out = t.toString();
				set.add(t.toString());
				continue;
			}
			if (!set.contains(t.toString())) {
				set.add(t.toString());
				out+=" "+t.toString();
			}
		}
		context.write(key, new Text(out));
	}
	
}

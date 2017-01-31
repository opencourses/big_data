package com.alangiu.bigdata.hadoop;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerImpl extends Reducer<
                Text, Text,   
                Text, NullWritable> {
	
    @Override
    protected void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
    	HashSet<String> set = new HashSet<>();
    	String out = "";
    	for (Text v: values) {
    		if (!set.contains(v.toString())) {
    			out+=v.toString()+" ";
    			set.add(v.toString());
    		}
    	}
    	context.write(new Text(out), NullWritable.get());
    }
}

package com.alangiu.bigdata.hadoop;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerImpl extends Reducer<
                Text, Text,   
                Text, Text> {
    
    @Override
    protected void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
    	String out = "";
    	HashSet<String> map = new HashSet<>();
    	for (Text v: values) {
    		if (!map.contains(v.toString())) {
    			out+=v+" ";
    			map.add(v.toString());
    		}
    	}
    	context.write(new Text(key+":"), new Text(out));
    }
}

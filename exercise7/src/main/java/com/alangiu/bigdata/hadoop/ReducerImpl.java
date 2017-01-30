package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerImpl extends Reducer<
                Text, Text,   
                Text, Text> {
    
    @Override
    protected void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
    	String out = null;
    	for (Text t : values) {
    		if (out == null) {
    			out = t.toString();
    			continue;
    		}
    		out = out.concat(", "+t);
    	}
    	context.write(key, new Text(out));
    }
}

package com.alangiu.bigdata.hadoop;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerImpl extends Reducer<
                Text, Text,   
                Text, NullWritable> {
	
	private String user = "";
	private HashSet<String> finalset;
	
	@Override
	protected void setup(Context context) {
		user = context.getConfiguration().get(DriverImpl.USER);
		finalset = new HashSet<>();
	}
	
    @Override
    protected void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
    	Boolean friends = false;
    	HashSet<String> set = new HashSet<>();
    	for (Text t: values) {
    		if (user.equals(t.toString())) {
    			friends = true;
    			continue;
    		}
    		if (!set.contains(t.toString())) {
    			set.add(t.toString());
    		}
    	}
    	if (friends) {
    		finalset.addAll(set);
    	}
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	String out = "";
    	for (String s: finalset) {
    		out+=s+" ";
    	}
		context.write(new Text(out), NullWritable.get());
    }
    
    
}

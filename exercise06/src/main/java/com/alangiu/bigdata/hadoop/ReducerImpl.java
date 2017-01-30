package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerImpl extends Reducer<
                Text, DoubleWritable,   
                Text, Text> {
    
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {
    	Double min = null;
    	Double max = null;
    	
    	for (DoubleWritable dw: values) {
    		if (min == null) {
    			min = dw.get();
    			max = dw.get();
    			continue;
    		}
    		if (dw.get() < min) {
    			min = dw.get();
    		} else if (dw.get() > max) {
    			max = dw.get();
    		}
    	}
    	context.write(key, new Text("min="+min+"_max="+max));
    }
}

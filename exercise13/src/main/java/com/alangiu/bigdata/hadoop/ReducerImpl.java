package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerImpl extends Reducer<
                NullWritable, Text,   
                Text, IntWritable> {
    
	private int top1 = 0;
	private String top1_value = "";
	private int top2 = 0;
	private String top2_value = "";
	
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
    	for (Text t : values) {
    		String[] v = t.toString().split("_");
    		int n1 = Integer.parseInt(v[0]);
    		int n2 = Integer.parseInt(v[2]);
    		if (n1 >= top2) {
    			if (n1 >= top1) {
    				top1 = n1;
    				top1_value = v[1];
    				if (n2 >= top2) {
    					top2 = n2;
    					top2_value = v[3];
    				}
    			} else {
    				top2 = n1;
    				top2_value = v[1];
    			}
    		}
    	}
    	context.write(new Text(top1_value), new IntWritable(top1));
    	context.write(new Text(top2_value), new IntWritable(top2));
    }
    
}

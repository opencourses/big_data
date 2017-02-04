package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class Reducer1Impl extends Reducer<
                Text, IntWritable,   
                Text, IntWritable> {
    
    @SuppressWarnings("unused")
	@Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
    		int num = 0;
    		for (IntWritable i : values) { 
    			num++; // it's faster
    		}
    		context.write(key, new IntWritable(num));
    }
}

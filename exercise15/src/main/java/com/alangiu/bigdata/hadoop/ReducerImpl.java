package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerImpl extends Reducer<
                Text, NullWritable,   
                Text, IntWritable> {
    
	private int num = 0;
	
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values,
        Context context) throws IOException, InterruptedException {
    	context.write(key, new IntWritable(num++));
    }
}

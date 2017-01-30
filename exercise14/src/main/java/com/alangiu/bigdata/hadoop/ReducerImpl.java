package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerImpl extends Reducer<
                Text, NullWritable,   
                Text, NullWritable> {
    
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values,
        Context context) throws IOException, InterruptedException {
    	context.write(key, NullWritable.get());
    }
}

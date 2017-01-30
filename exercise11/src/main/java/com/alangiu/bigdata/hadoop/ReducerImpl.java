package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerImpl extends Reducer<
                Text, DoubleWritable,   
                Text, DoubleWritable> {
    
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {
    	Average avg = new Average();
    	for (DoubleWritable v : values) {
    		avg.sum += v.get();
    		avg.num++;
    	}
    	context.write(key, new DoubleWritable(avg.calculate()));
    }
}

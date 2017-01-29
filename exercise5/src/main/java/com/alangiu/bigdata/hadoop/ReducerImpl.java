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
    	double sum = 0.0;
    	int num = 0;
    	for (DoubleWritable v: values) {
    		sum += v.get();
    		num++;
    	}
    	Double avg = new Double(sum/num);
    	context.write(key, new DoubleWritable(avg));
    }
}

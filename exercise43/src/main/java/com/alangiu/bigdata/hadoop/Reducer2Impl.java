package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class Reducer2Impl extends Reducer<
                Text, DoubleWritable,   
                Text, DoubleWritable> {
    
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {
    	Double sum = 0.0;
    	Integer num = 0;
    	for (DoubleWritable d : values){
    		sum+=d.get();
    		num++;
    	}
    	
    	context.write(key, new DoubleWritable(sum/num));
    }
}

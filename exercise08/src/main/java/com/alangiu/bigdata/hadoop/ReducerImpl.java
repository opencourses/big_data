package com.alangiu.bigdata.hadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerImpl extends Reducer<
                LongWritable, DoubleWritable,   
                Text, DoubleWritable> {
    
	private final SimpleDateFormat yearmonth = new SimpleDateFormat("yyyy-MM");

	
    @Override
    protected void reduce(LongWritable key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {
    	double sum = 0.0;
    	Date d = new Date();
    	d.setTime(key.get());
    	
    	for (DoubleWritable v: values) {
    		sum+=v.get();
    	}
    	
    	context.write(new Text(yearmonth.format(d)), new DoubleWritable(sum));
    }
}

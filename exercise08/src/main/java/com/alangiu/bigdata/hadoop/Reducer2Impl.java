package com.alangiu.bigdata.hadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class Reducer2Impl extends Reducer<
                LongWritable, DoubleWritable,   
                Text, DoubleWritable> {
    
	private final SimpleDateFormat year = new SimpleDateFormat("yyyy");

	
    @Override
    protected void reduce(LongWritable key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {
    	double sum = 0.0;
    	int num = 0;
    	Date d = new Date();
    	d.setTime(key.get());
    	
    	for (DoubleWritable v: values) {
    		sum+=v.get();
    		num++;
    	}
    	
    	double avg = sum/num;
    	context.write(new Text(year.format(d)), new DoubleWritable(avg));
    }
}

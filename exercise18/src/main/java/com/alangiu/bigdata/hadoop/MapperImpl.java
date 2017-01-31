package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

class MapperImpl extends Mapper<
                    LongWritable, Text,
                    Text, NullWritable> {
    
	private MultipleOutputs<Text, NullWritable> mo = null;
	
	@Override
	protected void setup(Context context) {
		mo = new MultipleOutputs<>(context);
	}
	
	@Override
    protected void map(LongWritable key, Text value,
            Context context) throws IOException, InterruptedException {
    	String[] values = value.toString().split(",");
    	Double d = Double.parseDouble(values[3]);
    	if (d > 30) {
    		mo.write(DriverImpl.HIGH, value, NullWritable.get());
    	} else {
    		mo.write(DriverImpl.NORMAL, value, NullWritable.get());
    	}
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mo.close();
	}
}

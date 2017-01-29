package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperImpl extends Mapper<
                    LongWritable, Text,
                    Text, IntWritable> {
    
    protected void map(LongWritable key, Text value,
            Context context) throws IOException, InterruptedException {
    		String v = value.toString().replaceAll(",", "\t");
    		String values[] = v.split("\\s+");
    		Double threshold = context.getConfiguration().getDouble(DriverImpl.THRESHOLD, 0.0);
    		if (Double.parseDouble(values[2]) > threshold) {
    			context.write(new Text(values[0]), new IntWritable(1));
    		}
    		
    		
    }
}

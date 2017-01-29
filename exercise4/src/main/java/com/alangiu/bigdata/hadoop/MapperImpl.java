package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperImpl extends Mapper<
                    LongWritable, Text,
                    Text, Text> {
    
    protected void map(LongWritable key, Text value,
            Context context) throws IOException, InterruptedException {
    	String v = value.toString().replaceAll(",","\t");
    	String[] vector = v.split("\\s+");
    	if (Double.parseDouble(vector[2]) >= context.getConfiguration().getDouble(DriverImpl.THRESHOLD, 0.0)) {
    		context.write(new Text(vector[0]), new Text(vector[1]));
    	}
    }
}

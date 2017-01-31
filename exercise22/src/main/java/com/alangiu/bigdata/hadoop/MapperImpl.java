package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperImpl extends Mapper<
                    LongWritable, Text,
                    Text, Text> {
    
	@Override
    protected void map(LongWritable key, Text value,
            Context context) throws IOException, InterruptedException {
    	String[] values = value.toString().split(",");
    	context.write(new Text(values[0]), new Text(values[1]));
    	context.write(new Text(values[1]), new Text(values[0]));
    }
}

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
    	String[] values= value.toString().split(" ");
    	for (String v: values) {
    		v = v.replaceAll("\\W", "");
    		context.write(new Text(v), new IntWritable(1));
    	}
    }
}

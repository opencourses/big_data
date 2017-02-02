package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class Mapper1Impl extends Mapper<
                    LongWritable, Text,
                    Text, Text> {
    	
	@Override
    protected void map(LongWritable key, Text value,
            Context context) throws IOException, InterruptedException {

    	String[] str = value.toString().split(",");
    	// remove the schema
    	if (!str[0].equals("Id")) {
    		context.write(new Text(str[2]), new Text(str[1]+":"+str[6]));    
    	}
    		
    }
}

package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class Mapper2Impl extends Mapper<
                    Text, Text,
                    Text, DoubleWritable> {
    
	@Override
    protected void map(Text key, Text value,
            Context context) throws IOException, InterruptedException {
    		context.write(key, new DoubleWritable(Double.parseDouble(value.toString())));
    }
}

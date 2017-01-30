package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class Mapper1Impl extends Mapper<
                    LongWritable, Text,
                    Text, DoubleWritable> {

	@Override
    protected void map(LongWritable key, Text value,
            Context context) throws IOException, InterruptedException {
    	String[] values = value.toString().split(",");
    	Double d = Double.parseDouble(values[3]);
    	context.write(new Text(values[1]), new DoubleWritable(d));
    }
}

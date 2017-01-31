package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperImpl extends Mapper<
                    LongWritable, Text,
                    Text, NullWritable> {
    
	@Override
    protected void map(LongWritable key, Text value,
            Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		Double d = Double.parseDouble(values[3]);
		if (d > 30.0) {
			context.write(value, NullWritable.get());
		}
    }
}

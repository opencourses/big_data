package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperImpl extends Mapper<
                    LongWritable, Text,
                    Text, Text> {
    
	private String user = null;
	
	@Override
	protected void setup(Context context) {
		this.user = context.getConfiguration().get(DriverImpl.USER);
	}
	
    protected void map(LongWritable key, Text value,
            Context context) throws IOException, InterruptedException {
    	String[] users = value.toString().split(",");
    	if (user.equals(users[0])) {
    		context.write(new Text(users[0]), new Text(users[1]));
    	} else if (user.equals(users[1])) {
    		context.write(new Text(users[1]), new Text(users[0]));
    	}
    }
}

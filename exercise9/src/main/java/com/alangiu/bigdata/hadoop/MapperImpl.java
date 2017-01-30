package com.alangiu.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperImpl extends Mapper<
                    LongWritable, Text,
                    Text, IntWritable> {
	
	private HashMap<String, Integer> map;
	
	@Override
	protected void setup(Context context) {
		map = new HashMap<>();
	}
	
    @Override
    protected void map(LongWritable key, Text value,
            Context context) {
    	String[] values = value.toString().split("\\s+");
    	for (String v: values) {
    		v=v.toLowerCase().replaceAll(",|\\.", "");
    		Integer next = map.get(v);
    		if (next == null) {
    			next = 1; 
    		} else {
    			next++;
    		}
    		map.put(v, next);
    	}
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	for (Entry<String, Integer> e : map.entrySet()) {
    		context.write(new Text(e.getKey()), new IntWritable(e.getValue()));
    	}
    }
}

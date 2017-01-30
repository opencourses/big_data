package com.alangiu.bigdata.hadoop;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperImpl extends Mapper<
                    LongWritable, Text,
                    Text, Text> {
    
    protected void map(LongWritable key, Text value,
            Context context) throws IOException, InterruptedException {
    	String[] values = value.toString().split("\\s+");
    	HashSet<String> set = new HashSet<>();
    	set.add("or"); // if the elements are present in the set they are not printed
    	set.add("and");
    	boolean first = true;
    	Text id = null;
    	for (String v: values) {
    		if (first) {
    			id = new Text(v);
    			first = false;
    			continue;
    		}
    		v = v.trim().toLowerCase();
    		if (set.contains(v)) {
    			continue;
    		}
    		set.add(v);
    		context.write(new Text(v), id);
    	}
    }
}

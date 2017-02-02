package com.alangiu.bigdata.hadoop;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class Reducer1Impl extends Reducer<
                Text, Text,   
                Text, DoubleWritable> {
    
    @Override
    protected void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
    	// I dont't need more the key
    	// I need first to calculate the average of rating for the user
    	
    	Double ratingssum = 0.0;
    	Integer num = 0;
    	List<String> products = new LinkedList<>();
    	List<Double> ratings = new LinkedList<>();
    	for (Text t: values) {
    		String[] s = t.toString().split(":");
    		Double rating = Double.parseDouble(s[1]);
    		ratingssum += rating;
    		products.add(num, s[0]);
    		ratings.add(num, rating);
    		num++;
    	}
    	
    	Double avg = ratingssum / num;
    	for (int i = 0; i< products.size(); i++) {
    		Double v = ratings.get(i) - avg;
    		context.write(new Text(products.get(i)), new DoubleWritable(v));
    	}
    }
}

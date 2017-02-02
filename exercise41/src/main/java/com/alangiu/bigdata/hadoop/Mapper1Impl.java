package com.alangiu.bigdata.hadoop;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVReader;

class Mapper1Impl extends Mapper<
                    LongWritable, Text,
                    NullWritable, Text> {
    
		
	@Override
    protected void map(LongWritable key, Text value,
            Context context) throws IOException, InterruptedException {
		CSVReader reader = new CSVReader(new StringReader(value.toString()+"\n"));
		if (reader.getLinesRead() == 0) {
			reader.close();
			return;
		}
		String[] line = reader.readNext();
		reader.close();
		if (line.length < 5) {
			return;
		}
    	String review = line[4].replaceAll(",|\\.|;|:|\\?|!|\"", "").trim();
    	context.write(NullWritable.get(), new Text(review));
    }
}

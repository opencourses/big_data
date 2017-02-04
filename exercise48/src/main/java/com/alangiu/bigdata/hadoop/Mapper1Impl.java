package com.alangiu.bigdata.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class Mapper1Impl extends Mapper<
                    LongWritable, Text,
                    Text, IntWritable> {
    
	private SimpleDateFormat sdf;
	
	@Override
	protected void setup(Context c) {
		sdf = new SimpleDateFormat("yyyyMMdd_HH:mm");
	}
	
	@Override
    protected void map(LongWritable key, Text value,
            Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split(",");
		Long diff = null;
		try {
			diff = compareDates(split[3], split[2]);
			if (diff > 10) {
				context.write(new Text(split[1]), new IntWritable(1));
			}
		} catch (Exception e ){
			return;
		}
    }
	
	private long compareDates(String date1, String date2) throws ParseException {
		Date d1 = sdf.parse(date1);
		Date d2 = sdf.parse(date2);
		long diff = d1.getTime() -d2.getTime();
		long diffMinutes = diff / (60*1000);
		return diffMinutes;
	}
}

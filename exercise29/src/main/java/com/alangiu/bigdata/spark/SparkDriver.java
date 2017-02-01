package com.alangiu.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("exercise29");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> readRDD = sc.textFile(inputPath);

		JavaDoubleRDD valuesRDD = readRDD.mapToDouble(new DoubleFunction<String>() {
			@Override
			public double call(String arg0) throws Exception {
				String[] s = arg0.split(",");
				return Double.parseDouble(s[2]);
			}
		});
		
		final Double max = valuesRDD.max();
		
		JavaRDD<String> finalRDD = readRDD.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String arg0) throws Exception {
				String[] s = arg0.split(",");
				Double d = Double.parseDouble(s[2]);
				if (d.equals(max)) { 
					return true;					
				}
				return false;
			}
		});
		
		finalRDD.saveAsTextFile(outputPath);
		
		sc.close();
	}
}

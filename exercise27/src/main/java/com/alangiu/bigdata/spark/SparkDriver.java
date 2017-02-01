package com.alangiu.bigdata.spark;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		
		SparkConf conf = new SparkConf().setAppName("exercise27");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> readRDD = sc.textFile(inputPath);

		JavaDoubleRDD pollutionDoubleRDD = readRDD.mapToDouble(new DoubleFunction<String>() {
			@Override
			public double call(String arg0) throws Exception {
				String[] str = arg0.split(",");
				return Double.parseDouble(str[2]);
			}
			
		});
		
		Double max = pollutionDoubleRDD.max();
		
		System.out.println("the max is "+ max);

		sc.close();
	}
}

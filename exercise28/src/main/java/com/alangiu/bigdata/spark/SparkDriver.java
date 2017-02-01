package com.alangiu.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;

import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];

		SparkConf conf = new SparkConf().setAppName("exercise28");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> readRDD = sc.textFile(inputPath);

		JavaDoubleRDD pollutionDoubleRDD = readRDD.mapToDouble(new DoubleFunction<String>() {

			@Override
			public double call(String arg0) throws Exception {
				String[] s = arg0.split(",");
				return Double.parseDouble(s[2]);
			}
			
		});
		
		List<Double> top = pollutionDoubleRDD.top(3);

		for (Double d: top) {
			System.out.println(d);
		}

		sc.close();
	}
}

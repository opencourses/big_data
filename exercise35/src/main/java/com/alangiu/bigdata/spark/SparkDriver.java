package com.alangiu.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("exercise35");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> readRDD = sc.textFile(inputPath);

		JavaRDD<String> filteredRDD = readRDD.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String arg0) throws Exception {
				String[] s = arg0.split(",");
				return Double.parseDouble(s[2]) > 50 ? true : false;
			}
		});
		
		JavaPairRDD<String, Integer> pairRDD = filteredRDD.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				String[] s = arg0.split(",");
				return new Tuple2<String, Integer>(s[0], 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0+arg1;
			}
		});
		
		JavaPairRDD<Integer, String> invertedRDD = 
				pairRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> arg) throws Exception {
						return new Tuple2<Integer, String>(arg._2, arg._1);
					}
		});
		
		invertedRDD.sortByKey(false).saveAsTextFile(outputPath);

		sc.close();
	}
}

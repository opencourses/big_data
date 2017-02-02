package com.alangiu.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath1 = args[0];
		String inputPath2 = args[1];
		String outputPath1 = args[2];
		String outputPath2 = args[3];
		final Double threshold = Double.parseDouble(args[4]);
		
		SparkConf conf = new SparkConf().setAppName("exercise47");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> purchasesRDD = sc.textFile(inputPath1).cache();
		JavaRDD<String> booksRDD = sc.textFile(inputPath2);

		/*********
		 * PART A - SELECT THE BOOKIDS OF THE EXPENSIVE NEVER SOLD BOOKS
		 *******/
		JavaRDD<String> purchRDD = purchasesRDD.map(new Function<String, String>() {
			@Override
			public String call(String arg0) throws Exception {
				String[] split = arg0.split(",");
				return split[1];
			}
		}).distinct();

		JavaRDD<String> expensiveBooksRDD = booksRDD.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String arg0) throws Exception {
				String[] split = arg0.split(",");
				if (Double.parseDouble(split[3]) >= 30.0) {
					return true;
				}
				return false;
			}
		}).map(new Function<String, String>() {
			@Override
			public String call(String arg0) throws Exception {
				String[] split = arg0.split(",");
				return split[0];
			}
		});

		JavaRDD<String> result1 = expensiveBooksRDD.subtract(purchRDD);
		result1.saveAsTextFile(outputPath1);

		/******
		 * PART B - SELECT THE CUSTOMER WITH A PROPENSION FOR CHEAP PURCHASES
		 ***/
		purchasesRDD.mapToPair(new PairFunction<String, String, Count>() {
			@Override
			public Tuple2<String, Count> call(String arg0) throws Exception {
				String[] split = arg0.split(",");
				Double value = Double.parseDouble(split[3]);
				Count count;
				if (value < 10.0) {
					count = new Count(1, 1);
				} else {
					count = new Count(1, 0);
				}
				return new Tuple2<String, Count>(split[0], count);
			}
		}).reduceByKey(new Function2<Count, Count, Count>() {
			@Override
			public Count call(Count a, Count b) throws Exception {
				return new Count(a.total + b.total, a.cheap + b.cheap);
			}
		}).mapToPair(new PairFunction<Tuple2<String, Count>, String, Double>() {
			@Override
			public Tuple2<String, Double> call(Tuple2<String, Count> arg0) throws Exception {
				Double avg = new Double(arg0._2.cheap) / new Double(arg0._2.total);
				return new Tuple2<String, Double>(arg0._1, avg);
			}
		}).filter(new Function<Tuple2<String, Double>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, Double> v) throws Exception {
				return v._2 > threshold ? true : false; 
			}
		}).sortByKey().map(new Function<Tuple2<String, Double>, String> () {
			@Override
			public String call(Tuple2<String, Double> arg0) throws Exception {
				return arg0._1;
			}
		}).saveAsTextFile(outputPath2);
		
		sc.close();
	}
}

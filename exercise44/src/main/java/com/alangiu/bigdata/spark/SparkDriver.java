package com.alangiu.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("exercise44");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> readRDD = sc.textFile(inputPath);

		readRDD.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String arg0) throws Exception {
				String[] str = arg0.split(",");
				if (str[0].equals("Id")) {
					return false;
				}
				return true;
			}
			
		}).mapToPair(new PairFunction<String, String, Review>() {

			@Override
			public Tuple2<String, Review> call(String arg0) throws Exception {
				String[] str = arg0.split(",");
				return new Tuple2<String, Review>(str[2], new Review(str[1], Double.parseDouble(str[6])));
			}
		}).groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Review>>, String, Double>() {

			@Override
			public Iterator<Tuple2<String, Double>> call(Tuple2<String, Iterable<Review>> arg0) throws Exception {
				List<Tuple2<String, Double>> list = new ArrayList<>();
				Double sum = 0.0;
				Integer num = 0;
				for (Review r : arg0._2) {
					sum += r.rating;
					num++;
				}
				Double avg = sum/num;
				
				for (Review r : arg0._2) {
					list.add(new Tuple2<String, Double> (r.prod_id, r.rating-avg));
				}
				return list.iterator();
			}
		}).groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<Double>>, String, Double>() {

			@Override
			public Tuple2<String, Double> call(Tuple2<String, Iterable<Double>> arg0) throws Exception {
				Double sum = 0.0;
				Integer num = 0;
				for (Double d: arg0._2) {
					sum += d;
					num++;
				}
				return new Tuple2<String, Double>(arg0._1, sum/num);
			}
		}).saveAsTextFile(outputPath);

		sc.close();
	}
}

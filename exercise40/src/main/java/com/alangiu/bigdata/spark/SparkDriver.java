package com.alangiu.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("exercise40");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> readRDD = sc.textFile(inputPath);

		JavaPairRDD<Integer, Tuple2<Integer,String>> windowsItemRDD = 
				readRDD.flatMapToPair(new PairFlatMapFunction<String, Integer, Tuple2<Integer, String>>() {

					@Override
					public Iterable<Tuple2<Integer, Tuple2<Integer, String>>> call(String arg0) throws Exception {
						List<Tuple2<Integer, Tuple2<Integer, String>>> list = new ArrayList<>();
						String[] s = arg0.split(",");
						Integer timestamp = Integer.parseInt(s[0]);
						list.add(new Tuple2<Integer, Tuple2<Integer, String>>(timestamp, new Tuple2<Integer, String>(timestamp, s[1])));
						list.add(new Tuple2<Integer, Tuple2<Integer, String>>(timestamp-60, new Tuple2<Integer, String>(timestamp-60, s[1])));
						list.add(new Tuple2<Integer, Tuple2<Integer, String>>(timestamp-120, new Tuple2<Integer, String>(timestamp-120, s[1])));
						return list;
					}
		});
		
		windowsItemRDD.groupByKey().filter(new Function<Tuple2<Integer, Iterable<Tuple2<Integer, String>>>, Boolean>() {

			@Override
			public Boolean call(Tuple2<Integer, Iterable<Tuple2<Integer, String>>> arg) throws Exception {
				int timestamp = arg._1;
				int temp1 = 0;
				int temp2 = -1;
				int temp3 = -2;
				for (Tuple2<Integer, String> i:arg._2) {
					if (i._1 == timestamp) {
						temp1 = i._1;
					} else if (i._1 == timestamp+60) {
						temp1 = i._1;
					} else if (i._1 == timestamp+120) {
						temp1 = i._1;
					}
				}
				if (temp3 >= temp2 && temp2 >= temp1) {
					return true;
				}
				return false;
			}
		}).saveAsTextFile(outputPath);

		sc.close();
	}
}

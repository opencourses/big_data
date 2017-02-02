package com.alangiu.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String questionsPath = args[0];
		String answersPath = args[1];
		String outputPath = args[2];

		SparkConf conf = new SparkConf().setAppName("exercise37");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> questions = sc.textFile(questionsPath);
		JavaRDD<String> answers = sc.textFile(answersPath);

		JavaPairRDD<String, String> questionsPair = questions.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] s = arg0.split(",");
				return new Tuple2<String, String>(s[0], s[2]);
			}
			
		});
		
		JavaPairRDD<String, String> answersPair = answers.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] s = arg0.split(",");
				return new Tuple2<String, String>(s[1], s[3]);
			}
		});
		
		questionsPair.cogroup(answersPair).saveAsTextFile(outputPath);
		
		sc.close();
	}
}

package com.smang.spark.java8.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Word count").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> inputData = sc.textFile("data/proverbs.txt");
        JavaPairRDD<String, Integer> flattenPairs = inputData.flatMapToPair(text -> Arrays.asList(text.split(" ")).stream().map(word -> new Tuple2<String, Integer>(word,1)).iterator());
        JavaPairRDD<String, Integer> wordCountRDD = flattenPairs.reduceByKey((v1, v2) -> v1+v2);
        wordCountRDD.saveAsTextFile("data/out/word-count");



    }
}

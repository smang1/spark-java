package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Demonstrates CountByKey with an example
 *  Counts the occurance of each key in a pair RDD and returns a Map with key being the key of the element in the RDD
 *  and count being the number of occurrences of the specific key
 */
public class CountByKeyExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CountByKey Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, String> inputPairRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("A", "a"),
                new Tuple2<String, String>("B", "b"),
                new Tuple2<String, String>("C", "C"),
                new Tuple2<String, String>("X", "x"),
                new Tuple2<String, String>("C", "c"),
                new Tuple2<String, String>("A", "a"),
                new Tuple2<String, String>("A", "1"),
                new Tuple2<String, String>("a", "a")
                )
        );
        inputPairRDD.countByKey().forEach((x,y) -> System.out.println(String.format("Key: %s, Num Occurrences: %s", x,y)));
    }
}

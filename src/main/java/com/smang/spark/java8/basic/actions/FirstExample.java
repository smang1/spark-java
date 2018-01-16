package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Demonstrates  First action
 * Returns the first element of the RDD
 */
public class FirstExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("First Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 2, 6, 5, 1, 2, 1, 1, 90, 100));
        Integer firstValue=inputRDD.first();
        System.out.println(String.format("firstValue: %s",firstValue));
    }
}

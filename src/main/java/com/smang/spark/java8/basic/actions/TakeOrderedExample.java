package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Demonstrates TakeOrdered Action
 * Returns n elements of an RDD after applying the specified Ordering
 */
public class TakeOrderedExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TakeOrdered Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 2, 6, 5, 1, 2, 1, 1, 90, 100));
        List<Integer> takeValues1=inputRDD.takeOrdered(5);
        List<Integer> takeValues2=inputRDD.takeOrdered(5, Comparator.reverseOrder());
        System.out.println(String.format("takeValues1: %s, takeValues2: %s",takeValues1, takeValues2));
    }
}

package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Demonstrates Max with an example
 *
 * returns the max value in an RDD, the comparison of elements is based on the comparator give as argument
 */
public class MaxExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Max Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 2, 6, 5, 1, 2, 1, 1, 90, 100));
        Integer maxValue=inputRDD.max(Comparator.naturalOrder());
        Integer minValue=inputRDD.max(Comparator.reverseOrder());
        System.out.println(String.format("maxValue: %s, minValue: %s",maxValue, minValue));
    }


}

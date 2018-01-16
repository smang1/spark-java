package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Demonstrates Min action
 * finds the minimum value amondg the RDD elements, the comparison of elements is based on the comparator give as argument
 *
 */
public class MinValue {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Min Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 2, 6, 5, 1, 2, 1, 1, 90, 100));
        Integer minValue=inputRDD.min(Comparator.naturalOrder());
        Integer maxValue=inputRDD.min(Comparator.reverseOrder());
        System.out.println(String.format("maxValue: %s, minValue: %s",maxValue, minValue));
    }
}

package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Counts the number of elements in an RDD across Partitions
 */
public class CountExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CountExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1,2,3,4));

        System.out.println(inputRDD.count());
    }
}

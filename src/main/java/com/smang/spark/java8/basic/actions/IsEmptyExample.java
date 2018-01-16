package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Demonstrates isEmpty action.
 * Returns true if an RDD is empty otherwise returns false
 */
public class IsEmptyExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("IsEmptyExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1,2,3,4));
        Boolean isEmpty1 = inputRDD.filter(x -> x.equals(5)).isEmpty();
        Boolean isEmpty2 = inputRDD.filter(x -> x.equals(3)).isEmpty();

        System.out.println("Is first RDD empty: " +isEmpty1);
        System.out.println("Is second RDD empty: " +isEmpty2);
    }
}

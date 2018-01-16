package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Demonstrates takeSample action
 *
 * Fetches elements randomly from an RDD
 * parameters
 *      WithReplacement: a boolean value
 *          true->  the same element might get picked more tan once (TO BE CONFIRMED)
 *          false -> one element will get picked only once (TO BE CONFIRMED)
 *      num: Number of elements to be picked
 *      seed: when the same number is set for the seed value for different takes, the same set of values are returned.
 *            Very helpful in sampling the same set of data for testing
 *
 */
public class TakeSampleExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TakeSample Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 2, 6, 5, 11, 28, 19, 71, 90, 100));
        List<Integer> takeValues1=inputRDD.takeSample(false,6);
        List<Integer> takeValues2=inputRDD.takeSample(true,6);
        List<Integer> takeValuesWithSeed1=inputRDD.takeSample(true,2, 1);
        List<Integer> takeValuesWithSeed2=inputRDD.takeSample(true,2, 1);
        List<Integer> takeValuesWithSeed3=inputRDD.takeSample(true,2, 1);
        System.out.println(String.format("takeValues1: %s,\n takeValues2: %s,\n takeValuesWithSeed1: %s,\n takeValuesWithSeed2: %s,\n takeValuesWithSeed3: %s ",takeValues1, takeValues2, takeValuesWithSeed1, takeValuesWithSeed2, takeValuesWithSeed3));
    }
}

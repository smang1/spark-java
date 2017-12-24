package com.smang.spark.java8.basic.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * This class demonstrates the Distinct Transformation
 * => Target RDD will contin only distinct elements
 */
public class DistinctExample {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setMaster("local").setAppName("DistinctExample");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> intRDD1 = sc.parallelize(Arrays.asList(1,2,3,4,1,9, 10,4, 15));
        JavaRDD<Integer> outRDD = intRDD1.distinct();
        outRDD.foreach(x-> System.out.println(x));
    }

}

package com.smang.spark.java8.basic.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * This calss demonstrates the Intersection Transformation
 *
 * => Creates the target RDD with the elements that are present in all the source RDDs
 */
public class IntersectionExample {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setMaster("local").setAppName("IntersectionExample");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> intRDD1 = sc.parallelize(Arrays.asList(1,2,3,4));
        JavaRDD<Integer> intRDD2 = sc.parallelize(Arrays.asList(1,3,5,7,9));
        JavaRDD<Integer> outRDD = intRDD1.intersection(intRDD2);
        outRDD.foreach(x-> System.out.println(x));
    }
}

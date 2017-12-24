package com.smang.spark.java8.basic.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * This class demonstrates the Cartesian Transformation
 *
 *=> The target RDD is the cartesian product of two RDDs
 * => Each element of the  source RDD 1 is paired with each element of the source RDD 2 resulting in a PairRDD
 * => if source RDD 1 is of type "X" and source RDD 2 is of Type "Y" Target RDD will be a pair RDD of Type <X, Y>
 */
public class CartesianExample {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setMaster("local").setAppName("IntersectionExample");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> srcRDD1 = sc.parallelize(Arrays.asList(1,2,3,4));
        JavaRDD<String> srcRDD2 = sc.parallelize(Arrays.asList("Hello", "Hi", "Bonjour"));
        JavaPairRDD<Integer,String> outRDD = srcRDD1.cartesian(srcRDD2);
        outRDD.foreach(x-> System.out.println(x));
    }
}

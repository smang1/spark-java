package com.smang.spark.java8.basic.actions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Demonstrates SaveAsTextFile action
 * writes the content of an RDD to a file (HDFS or local).
 * Each element of the RDD is first converted to a line of String before writing it into a file
 */
public class SaveAsTextFileExample {
    public static void main(String[] args) {
        String outPutDirectory = "data/out/satf";

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("SaveAsTextFile Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3); //3 partitions
        System.out.println("Number of partitions for the input RDD: " + inputRDD.getNumPartitions());

        //Writing the contents of the RDD to outPutDirectory
        inputRDD.saveAsTextFile(outPutDirectory);

        //Read the data written in the previous step
        JavaRDD<String> input = sc.textFile(outPutDirectory);
        input.foreach(x -> System.out.println("Read from file: " + x));
    }
}

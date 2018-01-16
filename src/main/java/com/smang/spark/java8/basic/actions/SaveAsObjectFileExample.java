package com.smang.spark.java8.basic.actions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Demonstrates SaveAsObjectFile with an example
 *
 * Writes the content of an RDD to Object file. While using SaveAsTextFile, the contents of the RDD
 * are deserialized where as here we can stored the serialized contents directly in an object file
 */
public class SaveAsObjectFileExample {
    public static void main(String[] args) {
        String outPutDirectory = "data/out/saof";

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("SaveAsObjectFile Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3); //3 partitions
        System.out.println("Number of partitions for the input RDD: " + inputRDD.getNumPartitions());

        //Writing the contents of the RDD to outPutDirectory
        inputRDD.saveAsObjectFile(outPutDirectory);

        //Read the data written in the previous step
        JavaRDD<Integer> input = sc.objectFile(outPutDirectory);
        input.foreach(x -> System.out.println("Read from object file: " + x));
    }
}

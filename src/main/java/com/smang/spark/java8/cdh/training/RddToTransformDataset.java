package com.smang.spark.java8.cdh.training;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RddToTransformDataset {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RddToTransformDataset").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> frostRoad = sc.textFile("data/in/cloudera/frostroad.txt");
        System.out.println(frostRoad.collect());

    }
}

package com.smang.spark.java8.storage;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

//TODO: KO while connecting to CDH VM, To investigate
public class ReadFromHFDS {
    final static Logger logger = Logger.getLogger(ReadFromHFDS.class);
    public static void main(String[] args) {
        String inputDir = "/user/training/test/";
        String file1 = "user1.csv";
        String file2 = "user2.csv";

        //Logger.getLogger("org").setLevel(Level.OFF);
        //Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("ReadFromHFDS Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        logger.info("Trying out textfile() option");
        //Read the data from a single file
        JavaRDD<String> input1 = sc.textFile("hdfs://127.0.0.1:8020/user/cloudera/test/user1.csv");
        System.out.println(String.format("Number of lines in the file <user1>: %s", input1.count()));
        //54310

    }
}

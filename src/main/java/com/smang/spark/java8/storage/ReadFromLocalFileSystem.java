package com.smang.spark.java8.storage;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Explains different ways to read files form local file system
 * textFile() -> Reads into an RDD of lines
 * wholeTextfile() -> Reads into a pairRDD with file name as the key and the lines as value
 */
public class ReadFromLocalFileSystem {
    final static Logger logger = Logger.getLogger(ReadFromLocalFileSystem.class);

    public static void main(String[] args) {
        String inputDir = "data/in";
        String file1 = "user1.csv";
        String file2 = "user2.csv";

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("ReadFromLocalFileSystem Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        logger.info("Trying out textfile() option");
        //Read the data from a single file
        JavaRDD<String> input1 = sc.textFile(inputDir + "/" + file1);
        System.out.println(String.format("Number of lines in the file <user1>: %s", input1.count()));

        //Read the data from a two files
        JavaRDD<String> input2 = sc.textFile(String.format("%1$s/%2$s,%1$s/%3$s", inputDir, file1, file2));
        System.out.println(String.format("Total number of lines in the file <user1 + user2>: %s", input2.count()));

        //Read the data from all the files of type ".csv"
        JavaRDD<String> input3 = sc.textFile(inputDir + "/*.csv");
        System.out.println(String.format("Total number of lines in all the  files <user1 + user2>: %s", input3.count()));

        logger.info("Trying out WholeTextfile() option");
        //Read the data from a single file
        JavaPairRDD<String,String> input4 = sc.wholeTextFiles(inputDir + "/" + file1);
        System.out.println(String.format("Number of files read: %s", input4.count()));

        //Read the data from a two files
        JavaPairRDD<String,String> input5 = sc.wholeTextFiles(String.format("%1$s/%2$s,%1$s/%3$s", inputDir, file1, file2));
        System.out.println(String.format("Total number of files read: %s", input5.count()));

        //Read the data from all the files of type ".csv"
        JavaPairRDD<String,String> input6 = sc.wholeTextFiles(inputDir + "/*.csv");
        System.out.println(String.format("Total number of files read: %s", input6.count()));

        //TODO Count the number of lines per file
    }
}

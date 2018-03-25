package com.smang.spark.java8.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class SparkHiveExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkHive Example").setMaster("local");
        SparkContext sc = new SparkContext(conf);
        HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc);
        Dataset<Row> df = hiveContext.sql("show tables");
        df.show();
    }
}

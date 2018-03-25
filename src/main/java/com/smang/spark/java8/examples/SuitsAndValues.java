package com.smang.spark.java8.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SuitsAndValues {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Word count").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String input = "";
        String output = "";

        JavaRDD<String> inputData = sc.textFile(input);
        JavaPairRDD<String, Integer> suitsAndValues = runETL(inputData);

        JavaPairRDD<String, Integer> counts = getCounts(suitsAndValues);

        counts.saveAsTextFile(output);


    }

    public static JavaPairRDD<String, Integer> getCounts(JavaPairRDD<String, Integer> suitsAndValues) {
        return suitsAndValues.reduceByKey((x, y) -> x+y);
    }

    public static JavaPairRDD<String, Integer> runETL(JavaRDD<String> inputData) {
        JavaPairRDD<String,Integer> suitsAndValues = inputData.mapToPair(w ->{
            String[] split = w.split("\t");
            int cardValue = Integer.parseInt(split[0]);
            String cardSuit = split[1];
            return new Tuple2<String, Integer>(cardSuit,cardValue);
        });

        return suitsAndValues;
    }
}

package com.smang.spark.java8.exceptionHandling;

import com.smang.spark.java8.storage.fileFormats.pojos.PersonalDetails;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;

@Slf4j
public class RejectionExample {

    public static void main(String[] args) {
        String inputFile = "data/in/PersonalDetailsERR.json";
        String outputPath = "data/out/PersonalDetailsWithoutERR.json";

        SparkConf conf = new SparkConf().setAppName("ReadWriteSpeciallyFormattedText").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<PersonalDetails> correctData = input.filter(line ->{try {
            new ObjectMapper().readValue(line, PersonalDetails.class);
            return true;
        }catch (Exception e){
            return false;
        }
        }).map(line -> new ObjectMapper().readValue(line, PersonalDetails.class));
        correctData.foreach(line -> System.out.println(line.getFirstName()));


        JavaRDD<String> rejectedData = input.filter(line ->{try {
            new ObjectMapper().readValue(line, PersonalDetails.class);
            return false;
        }catch (Exception e){
            return true;
        }
        });
        rejectedData.foreach(line -> System.out.println(line));

    }
}

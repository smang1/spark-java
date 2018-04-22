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
        JavaRDD<PersonalDetails> output = input.filter(line ->{try {
            PersonalDetails personalDetails = new ObjectMapper().readValue(line, PersonalDetails.class);
            return true;
        }catch (Exception e){
            return false;
        }
        }).map(line -> new ObjectMapper().readValue(line, PersonalDetails.class));
        output.foreach(line -> System.out.println(line));
       /* //TODO Use Map partition for better performance
        JavaRDD<PersonalDetails> personalDetails = inputDataset.toJavaRDD().map(line -> new ObjectMapper().readValue(line, PersonalDetails.class));
        personalDetails.foreach(x -> System.out.println(String.format("cid: %s, name: %s, dob: %s", x.getCid(), x.getFirstName(), x.getDateOfBirth())));
*/
    }
}

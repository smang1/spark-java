package com.smang.spark.java8.storage.fileFormats;

import com.smang.spark.java8.storage.fileFormats.pojos.PersonalDetails;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Demonstrates different ways of reading a JSON file in Spark
 *
 *
 */
public class ReadWriteJSONFiles {
    public static void main(String[] args) {
        String inputFile = "data/in/PersonalDetails.json";
        String outputPath = "data/out/ReadWriteJSONFiles";

        SparkSession ss = SparkSession.builder().master("local").appName("ReadWriteJSONFiles")
                .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse").getOrCreate();
        Dataset<String> inputDataset = ss.read().textFile(inputFile);

        //TODO Use Map partition for better performance
        JavaRDD<PersonalDetails> personalDetails = inputDataset.toJavaRDD().map(line -> new ObjectMapper().readValue(line, PersonalDetails.class));
        personalDetails.foreach(x -> System.out.println(String.format("cid: %s, name: %s, dob: %s", x.getCid(), x.getFirstName(), x.getDateOfBirth())));

        Dataset<Row> pdRows = ss.read().json(inputFile);
        pdRows.printSchema();
        pdRows.show();

        //Create Custom Schema -> Improved performance (elements of the data frame is not iterated twice)
        StructType customSchema = new StructType(new StructField[]{
                DataTypes.createStructField("cid",DataTypes.LongType,true),
                DataTypes.createStructField("county",DataTypes.StringType,true),
                DataTypes.createStructField("firstName",DataTypes.StringType,true),
                DataTypes.createStructField("sex",DataTypes.StringType,true),
                DataTypes.createStructField("year",DataTypes.StringType,true),
                DataTypes.createStructField("dateOfBirth",DataTypes.TimestampType,true)


        });

        Dataset<Row> pdRowsCustomSchema = ss.read().schema(customSchema).json(inputFile);
        pdRowsCustomSchema.printSchema();
        pdRowsCustomSchema.show();

        pdRowsCustomSchema.write().format("json").mode("overwrite").save(outputPath);


    }
}

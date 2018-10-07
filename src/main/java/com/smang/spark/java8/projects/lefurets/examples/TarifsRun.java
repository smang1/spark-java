package com.smang.spark.java8.projects.lefurets.examples;

import com.smang.spark.java8.projects.lefurets.domain.ProductMapper;
import com.smang.spark.java8.projects.lefurets.utils.SparkRunner;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class TarifsRun extends SparkRunner {
    private static SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
            .getOrCreate();

    static {
        sparkSession.udf().register("readableFormule", (UDF1<Integer, String>) ProductMapper::french, StringType);
    }

    public static void main(String[] args) {
        Dataset<Row> tarifs=sparkSession.read()
                .option("header",true)
                .option("inferSchema",true)
                .csv(PATH_TARIFS_CSV);

        System.out.println(tarifs.count());
        tarifs.show();
        tarifs.printSchema();

        averagePrime(tarifs);

    }

    public static Dataset<Row> averagePrime(Dataset<Row> tarifs){
        Dataset<Row> averagePrime=tarifs
                .filter( (FilterFunction<Row>) value-> value.getAs("assureur").equals("Mon SUPER assureur"))
                .groupBy("formule")
                .agg(org.apache.spark.sql.functions.avg("prime").as("average"))
                .withColumn("formuleReadable",org.apache.spark.sql.functions.callUDF("readableFormule", col("formule")))
                .orderBy(desc("average"));
        averagePrime.show();
return averagePrime;
    }
}

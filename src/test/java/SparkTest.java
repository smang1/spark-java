import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import com.smang.spark.java8.examples.SuitsAndValues;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Ignore;
import org.junit.Test;
import org.stringtemplate.v4.ST;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
@Ignore
public class SparkTest extends SharedJavaSparkContext implements Serializable {
    private static final long serialVersionUID = -5681683598336701496L;

    @Test
    public void verifyMapTest() {
        // Create and run the test
        // creating the input RDD
        List<String> input = Arrays.asList("1\tHeart", "2\tDiamonds");
        JavaRDD<String> inputRDD = jsc().parallelize(input);
        JavaPairRDD<String, Integer> result = SuitsAndValues.runETL(inputRDD);

        //Create Expected output
        List<Tuple2<String, Integer>> expectedOutput = Arrays.asList(new Tuple2<String, Integer>("Heart", 1),
                new Tuple2<String, Integer>("Diamonds", 2));
        JavaPairRDD<String, Integer> expectedOutputRDD = jsc().parallelizePairs(expectedOutput);

        //create ClassTag object.
        // This allows Scala to reflect the Object’s type correctly during the JavaPairRDD to JavaRDD conversion
        ClassTag<Tuple2<String, Integer>> tag = ClassTag$.MODULE$.apply(Tuple2.class);

        JavaRDDComparisons.assertRDDEquals(JavaRDD.fromRDD(JavaPairRDD.toRDD(result),tag),
                JavaRDD.fromRDD(JavaPairRDD.toRDD(expectedOutputRDD),tag));

    }
}

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.hive.test.TestHiveContext;
import org.junit.Ignore;
import org.junit.Test;
import org.apache.spark.sql.Row;
//implements Serializable
@Ignore
public class HiveTest extends SharedJavaSparkContext  {
    @Test
    public void verifyCreateTableTest() {
     // TestHiveContext hiveContext = new TestHiveContext(jsc().sc());
        
        HiveContext hc = new org.apache.spark.sql.hive.HiveContext(jsc().sc());
        hc.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
        hc.sql("LOAD DATA LOCAL INPATH '/opt/mapr/hive/hive-1.2/examples/files/kv1.txt' INTO TABLE src");
        hc.sql("FROM src SELECT key, value").collect();

      //  System.out.println(results.toString());
    }

}

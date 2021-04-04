package PackageDemo;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;

public class SparkSQL {
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQL")
                .getOrCreate();

        Dataset<String> firstDS = spark.read().text(args[0]).as(Encoders.STRING());
        Dataset<String> secondDS = spark.read().text(args[1]).as(Encoders.STRING());

        Dataset<String> firstDSN = firstDS.flatMap(s -> Arrays.asList(s.toLowerCase().split("\\s")).iterator(), Encoders.STRING()).filter(s -> !s.isEmpty());
        Dataset<String> secondDSN = secondDS.flatMap(s -> Arrays.asList(s.toLowerCase().split("\\s")).iterator(), Encoders.STRING()).filter(s -> !s.isEmpty());

        Dataset<Row> fisrtT = firstDSN.toDF("words");
        Dataset<Row> secondT = secondDSN.toDF("words");

        //  "EXCEPT" should "return all rows not present in the second dataset"
        fisrtT = fisrtT.except(secondT).sort("words").coalesce(1);

        // Add new column - length
        Dataset<Row> wordsATL = fisrtT.withColumn("length", length(col("words")));
        // Sort by "length" column desc
        wordsATL = wordsATL.sort(functions.desc("length"));

        wordsATL.show();
        
        wordsATL.toJavaRDD().saveAsTextFile(args[2]);
    }
}
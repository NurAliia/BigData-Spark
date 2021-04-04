package PackageDemo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class Spark {
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName("CompareFiles")
                .getOrCreate();

        JavaRDD<String> firstString = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<String> secondString = spark.read().textFile(args[1]).javaRDD();

        JavaRDD<String> firstWords = firstString.flatMap(s -> Arrays.asList(s.split("\\s")).iterator());
        JavaRDD<String> secondWords = secondString.flatMap(s -> Arrays.asList(s.split("\\s")).iterator());

        // Normalize words and get distinct elements of RDD
        JavaRDD<String> firstWordsNormalize = firstWords.map(s -> s.toLowerCase()).distinct();
        JavaRDD<String> secondWordsNormalize = secondWords.map(s -> s.toLowerCase()).distinct();

        JavaRDD<String> unique = firstWordsNormalize.subtract(secondWordsNormalize).sortBy(f -> f.length(), false, 1);

        System.out.println("sorted:");
        unique.foreach(item -> {
            System.out.println(item);
        });
        spark.stop();
    }
}
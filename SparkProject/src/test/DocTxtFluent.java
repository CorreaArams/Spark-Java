package test;

import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class DocTxtFluent {

    @SuppressWarnings("resource")
    public static void main(String[] args)
    {

        Logger.getRootLogger().setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile("src/resources/input2.txt")
            .map( sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase() )
            .filter( sentence -> sentence.trim().length() > 0 )
            .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
            .filter(word -> word.trim().length() > 0)
            .filter(word -> Util.isNotBoring(word))
            .mapToPair(word -> new Tuple2<String, Long>(word, 1L))
            .reduceByKey((value1, value2) -> value1 + value2)
            .mapToPair(tuple -> new Tuple2<Long, String> (tuple._2, tuple._1 ))
            .sortByKey(false)
            .take(10)
            .forEach(System.out::println);

        sc.close();
    }

}

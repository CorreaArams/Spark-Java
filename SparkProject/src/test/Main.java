package test;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple5;

public class Main {

    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(1);
        inputData.add(2);
        inputData.add(3);
        inputData.add(4);
        inputData.add(5);
        inputData.add(3);
        inputData.add(2);

       // Logger.getLogger("org.apache").setLevel(Level.WARN);
        //Logger.getRootLogger().setLevel(Level.ERROR);

        //RDD used to distributed data
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // JavaRDD comunicate to scala, implements scala itself
        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        for(Integer n: inputData) {
            System.out.println(n);
        }
        System.out.println("-----------------------------------");

        Integer sum = myRdd.reduce((a, b) -> a + b);
        System.out.println("the sum is: "+sum);

        Integer min = myRdd.reduce( (a, b) -> Math.min(a, b));
        System.out.println("the min is: "+min);

        Integer max = myRdd.reduce( (a, b) -> Math.max(a, b));
        System.out.println("the max is: "+max);


        JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);

        JavaRDD<IntegerWithSquareRoot> sqrtRdd = originalIntegers.map( value -> new IntegerWithSquareRoot(value));

        // JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalIntegers.map( value -> new Tuple2<>(value, Math.sqrt(value)));

        Tuple2 <Integer, Double> myValue = new Tuple2(9,3.0);

        //new Tuple5<>(4,3,2,1,2);
        // the max tuple you can use is Tuple22<>()


        sc.close();

    }

}

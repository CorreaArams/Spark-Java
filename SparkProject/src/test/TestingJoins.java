package test;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TestingJoins {
    public static void main(String[] args)
    {
        Logger.getRootLogger().setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> visitRaw = new ArrayList<>();
        visitRaw.add(new Tuple2<>(4,18));
        visitRaw.add(new Tuple2<>(6,4));
        visitRaw.add(new Tuple2<>(10,9));

        List<Tuple2<Integer, String>> visitorsRaw = new ArrayList<>();
        visitorsRaw.add(new Tuple2<>(1,"John"));
        visitorsRaw.add(new Tuple2<>(2,"Bob"));
        visitorsRaw.add(new Tuple2<>(3,"Alan"));
        visitorsRaw.add(new Tuple2<>(4,"Doris"));
        visitorsRaw.add(new Tuple2<>(5,"Marybelle"));
        visitorsRaw.add(new Tuple2<>(6,"Raquel"));

        JavaPairRDD<Integer,Integer> visits = sc.parallelizePairs(visitRaw);
        JavaPairRDD<Integer, String> visitors = sc.parallelizePairs(visitorsRaw);

        //inner join
        //JavaPairRDD<Integer, Tuple2<Integer,String>> joinedRdd = visits.join(visitors);
        //joinedRdd.foreach(e -> System.out.println(e));

        //Left Outer join
        //JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRdd = visits.leftOuterJoin(visitors);
        //joinedRdd.foreach(it -> System.out.println(it._2._2.orElse("blank").toUpperCase()  ));

        //Right Outer join
        //JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRdd = visits.rightOuterJoin(visitors);
        //joinedRdd.foreach(it -> System.out.println(" user " + it._2._2 + " had "+ it._2._1.orElse(0)+" visits"));
        
        //full outer join
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> joinedRdd = visits.cartesian(visitors);
        joinedRdd.foreach(x -> System.out.println(x));

        sc.close();

    }
}

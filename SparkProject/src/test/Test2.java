package test;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class Test2 {
        public static void main(String[] args) throws AnalysisException {
            Logger.getRootLogger().setLevel(Level.ERROR);

            SparkSession spark = SparkSession
                    .builder()
                    .appName("testingSpark")
                    .master("local[*]").config("spark.sql.warehouse.dir","file:///c:").getOrCreate();

            Dataset<Row> dataset = spark.read().option("header",true).csv("src/resources/people.txt");
            dataset.show();

            Dataset<Row> df = spark.read().json("src/resources/people.json");
            df.show();

            df.printSchema();

            df.select("name").show();
            df.select("age").show();

            // increment age + 1
            df.select(col("name"), col("age").plus(1)).show() ;

            // age greater than 21 gt(21)
            df.filter(col("age").gt(21)).show();

            // group by expression
            df.groupBy("age").count().show();

            // register as a temporary view
            df.createOrReplaceTempView("people");
            Dataset<Row> sqlDF = spark.sql("select * from people");
            sqlDF.show();

            // register as a global temp view
            df.createGlobalTempView("people");
            spark.sql("select * from global_temp.people").show();

            // global view is cross session, new session can access
            spark.newSession().sql("select * from global_temp.people").show();


        }
}

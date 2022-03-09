package test;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.io.Serializable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.AnalysisException;

public class SparkSessionDoc {
    public static void main(String[] args) throws AnalysisException {

        Logger.getRootLogger().setLevel(Level.ERROR);

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL example")
                .master("local[*]")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();


        runInferSchemaExample(spark);
        runDataset(spark);
        runProgrammaticSchemaExample(spark);

        spark.stop();
    }
    public static class Person implements Serializable {
        private String name;
        private String age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAge() {
            return age;
        }

        public void setAge(String age) {
            this.age = age;
        }
    }

    private static void runInferSchemaExample(SparkSession spark) {
        JavaRDD<Person> peopleRDD = spark.read().textFile("src/resources/people.txt")
                .javaRDD().map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(parts[1]);

                    return person;

                });

        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD,Person.class);
        peopleDF.createOrReplaceTempView("people");

        Dataset<Row> teenagerDF = spark.sql("select name from people where age between 13 and 19");

        // by index / position
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagerDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                stringEncoder);
        teenagerNamesByIndexDF.show();

        // by field "name"
        Dataset<String> TeenagerbyfieldDF = teenagerDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder);
        TeenagerbyfieldDF.show();
    }

    private static void runDataset(SparkSession spark){
        //create a instance of a bean class
        Person person = new Person();
        person.setName("Andreia");
        person.setAge("23");

        // Encoder<Person> Used to convert a JVM object of type "Person" to and from the internal Spark SQL representation
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeansDs = spark.createDataset(
               // .singletonList() This method returns an immutable list containing only the specified object
                Collections.singletonList(person),
                personEncoder
        );
        javaBeansDs.show();

    }
    private static void runProgrammaticSchemaExample(SparkSession spark) {
        // $example on:programmatic_schema$
        // Create an RDD
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("src/resources/people.txt", 1)
                .toJavaRDD();

        // The schema is encoded in a string
        String schemaString = "name age";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        // Apply the schema to the RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        // Creates a temporary view using the DataFrame
        peopleDataFrame.createOrReplaceTempView("people");

        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = spark.sql("SELECT name FROM people");

        // The results of SQL queries are DataFrames and support all the normal RDD operations
        // The columns of a row in the result can be accessed by field index or by field name
        Dataset<String> namesDS = results.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();
    }

}

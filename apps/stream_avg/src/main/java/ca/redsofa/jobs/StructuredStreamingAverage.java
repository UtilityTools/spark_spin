package ca.redsofa.jobs;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import ca.redsofa.domain.Person;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.StructType;


public final class StructuredStreamingAverage {
  private static String INPUT_DIRECTORY = "/home/richardr/streaming_test/input_files";

  public static void main(String[] args) throws Exception {
    System.out.println("Starting StructuredStreamingAverage job...");
    SparkSession spark = SparkSession
      .builder()
      .config("spark.eventLog.enabled", "false")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "2g")
      .appName("StructuredStreamingAverage")
      .getOrCreate();

    StructType personSchema = new StructType()
                                    .add("firstName", "string")
                                    .add("lastName", "string")
                                    .add("sex", "string")
                                    .add("age", "long");

    //Create DataFrame representing the stream of input files
    Dataset<Person> personStream = spark
      .readStream()
      .schema(personSchema)
      .json(INPUT_DIRECTORY)
      .as(Encoders.bean(Person.class));
       
      personStream.createOrReplaceTempView("people");
      Dataset<Row> ageAverage = spark.sql("SELECT AVG(age) as average_age, sex FROM people GROUP BY sex");

      StreamingQuery query = ageAverage.writeStream()
        .outputMode("complete")
        .format("console")
        .start();

    query.awaitTermination();
  }
}

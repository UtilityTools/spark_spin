/*
TODO :
-------
Person Object that supports
- Male, count
- Female, count 

- File stream instead of soket
- Average age Calculation
- Persist State
- Output to JSON

//Taken from Apache Spark Example sources... Will be modified with requirements above :

In current state , 
- run "nc -lk 9999" in one terminal
- run sh submit_job.sh in the other

- Type a phrase in the first terminal and hit return key
*/


package ca.redsofa.jobs;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;
import java.util.Iterator;


public final class StructuredStreamingAverage {
  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession
      .builder()
      .appName("StructuredStreamingAverage")
      .getOrCreate();

    // Create DataFrame representing the stream of input lines from connection to host:port
    Dataset<String> lines = spark
      .readStream()
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load().as(Encoders.STRING());

    // Split the lines into words
    Dataset<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String x) {
        return Arrays.asList(x.split(" ")).iterator();
      }
    }, Encoders.STRING());

    // Generate running word count
    Dataset<Row> wordCounts = words.groupBy("value").count();

    // Start running the query that prints the running counts to the console
    StreamingQuery query = wordCounts.writeStream()
      .outputMode("complete")
      .format("console")
      .start();

    query.awaitTermination();
  }
}

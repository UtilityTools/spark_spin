package ca.redsofa.jobs;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import ca.redsofa.domain.Person;
import ca.redsofa.udf.StringLengthUdf;
import static org.apache.spark.sql.functions.callUDF;

public class SimpleBatch {
    private static String INPUT_FILE = "/Users/richardr/dev/git/spark_spin/data";

    public static void main(String[] args) {
        SimpleBatch job = new SimpleBatch();
        job.startJob();
    }

    private void startJob( ){
        System.out.println("Stating Job...");

        long startTime = System.currentTimeMillis();	    

        //1 - Start the Spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Batch Job")
                .config("spark.eventLog.enabled", "false")
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
                .enableHiveSupport()
                .getOrCreate();

        //2 - Read in the input data
        Dataset<Person> sourceDS = spark.read()
                                     .json(INPUT_FILE)
                                     .as(Encoders.bean(Person.class));

        //3 - Create a temporary table so we can use SQL queries
        sourceDS.createOrReplaceTempView("people");

        //4 - Register a user defined function to calculate the length of a String
        StringLengthUdf.registerStringLengthUdf(spark);

        //5 - Create a new Dataset based on the source Dataset

        //Example of adding a calculated column with a user defined function. 
        //This section of the code is not really central to exploring the 
        //similarity between a batch and streaming jobs.

        //Creates a new Dataset based on the sourceDS
        //The new Dataset contains a enw field called name_length which 
        //is calculated with the call to our new UDF         
        Dataset<Row> newDS = sourceDS
                                .withColumn("name_length", 
                                    callUDF("stringLengthUdf", 
                                    sourceDS.col("firstName")));    		

        //6 - Show a few records in the newDS Dataset. Added a name_length column
        newDS.show();

        String sql = "SELECT " + 
                        "AVG(age) as average_age, " + 
                        "sex " + 
                     "FROM " +
                       "people " +  
                     "GROUP BY " + 
                       "sex" ;

        //7 - Calculate the average age by sex for our population using a SQL script
        Dataset<Row> ageAverage = spark.sql(sql);

        //8 - Show the average age table
        ageAverage.show();

        spark.stop();		
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println("Execution time in ms : " + elapsedTime);
    }
}
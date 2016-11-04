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
		SparkSession spark = SparkSession
				.builder()
				.appName("Simple Batch Job")
				.config("spark.eventLog.enabled", "false")
				.config("spark.driver.memory", "2g")
				.config("spark.executor.memory", "2g")
				.enableHiveSupport()
				.getOrCreate();


		Dataset<Person> sourceDS = spark.read().json(INPUT_FILE).as(Encoders.bean(Person.class));
		sourceDS.createOrReplaceTempView("people");

		//Example of adding a calculated column with a user defined function. This section of the code is not
		//really central to exploring the similarity between a batch and streaming jobs.
		StringLengthUdf.registerStringLengthUdf(spark);
		//Creates a new Dataset based on the sourceDS
		//The new Dataset contains a enw field called name_length which is calculated with the call to our new UDF 
		Dataset<Row> newDS = sourceDS.withColumn("name_length", callUDF("stringLengthUdf", sourceDS.col("firstName")));    		
		newDS.show();


		Dataset<Row> ageAverage = spark.sql("SELECT AVG(age) as average_age, sex FROM people GROUP BY sex");
		ageAverage.show();

		spark.stop();
		
	    long stopTime = System.currentTimeMillis();
	    long elapsedTime = stopTime - startTime;
	    System.out.println("Execution time in ms : " + elapsedTime);
	}
}

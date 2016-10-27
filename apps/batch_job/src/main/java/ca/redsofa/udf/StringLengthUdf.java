package ca.redsofa.udf;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;


public class StringLengthUdf{
  public static void registerStringLengthUdf(SparkSession spark){
    spark.udf().register("stringLengthUdf", new UDF1<String, Long>() {
      @Override
      public Long call(String str) { 
          if(str != null && !str.isEmpty()){
            return new Long(str.length());  
          }else{
            return 0L;
          }
        }
      }, DataTypes.LongType);
  }
}
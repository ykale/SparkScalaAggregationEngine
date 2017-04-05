package com.hadoophome.aggregation.core

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark._


object TriggerAggregation {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("CaseAggregationEngine");

    val SparkContext = new SparkContext(sparkConf);
    val sqlContext = new SQLContext(SparkContext)
  
    
    
    /**
     * Reading the data file
     */
     var df = sqlContext.read
    .format("com.databricks.spark.csv")
   // .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("C:\\Users\\kaleya01\\Desktop\\cars.csv")
    
    
    /**
     * Assigning schema to the alredy loaded data frames
     */
	
    val schemaString = "Brand Model Cost";
    
	val IPDSchema =
  StructType(
    schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
	

    
    //df.schema();
    
    (IPDSchema);
    
    
    println("Executing properly till df.show")
    df.show();
    println("Completed df.show")
  }

}
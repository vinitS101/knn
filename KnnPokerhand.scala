//package knn
//Work in progress

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object pokerhand {
  def main(args: Array[String]) 
  {
    // create Spark context with Spark configuration
    val conf = new SparkConf().setAppName("WordCount")//.setMaster("local[*]") //Use last part for Eclipse
    val sc = new SparkContext(conf)
    
    //Training, testing and output file locations from argument
    // location should be from hdfs (for shell) or workspace/examples for Eclipse
    //val trainingFile = sc.textFile(args(0))  --> Doing it in Mapper
    val testFile = sc.textFile(args(1))
    val outputFile = sc.textFile(args(2))

    //Value of k from end of argument
    k = args(3).toInt
    
    val lines = testFile.map(x => x.toString().split("\t")

	    

  }

  def map(args: Array[String], test: String)
  {
  	val trainingFile = sc.textFile(args(0))
  }

  def 
}

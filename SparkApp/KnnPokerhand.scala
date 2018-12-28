package pokerhand.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.collection.immutable.TreeMap
import scala.io.Source

object KnnPokerhand {
  
  val K = 5;
  //var KnnMap: TreeMap[Double, Double] = new TreeMap[Double, Double]
  //val testData: Array[Int] = Array(1,1,1,13,2,4,2,3,1,12,0)
  var testData = new Array[Double](11)
  val minSuit: Double = 1.0
  val maxSuit: Double = 4.0
  val minRank: Double = 1.0
  val maxRank: Double = 13.0
  
  // Normalises the value to a scale of 0 to 1.0
  def normalisedDouble(givenVal: Double, minVal: Double, maxVal: Double ) : Double = {
    return ((givenVal - minVal) / (maxVal - minVal));
  }
  
  // Takes a double and returns its squared value.
  def squaredDistance(givenVal: Double) : Double = {
    return (givenVal*givenVal);
  }
  
  // Takes ten pairs of values, finds the difference between the members
  // of each pair (using nominalDistance() for strings) and returns the sum of the squared differences as a double.
  def totalSquaredDistance(trainData: Array[Double]) : Double = {
    
    val s1Diff: Double = trainData(0) - testData(0)
    val s2Diff: Double = trainData(2) - testData(2)
    val s3Diff: Double = trainData(4) - testData(4)
    val s4Diff: Double = trainData(6) - testData(6)
    val s5Diff: Double = trainData(8) - testData(8)
    
    val r1Diff: Double = trainData(1) - testData(1)
    val r2Diff: Double = trainData(3) - testData(3)
    val r3Diff: Double = trainData(5) - testData(5)
    val r4Diff: Double = trainData(7) - testData(7)
    val r5Diff: Double = trainData(9) - testData(9)
    
    val rankDist: Double = squaredDistance(s1Diff) + squaredDistance(s2Diff) + squaredDistance(s3Diff) + squaredDistance(s4Diff) + squaredDistance(s5Diff)
    val suitDist: Double = squaredDistance(r1Diff) + squaredDistance(r2Diff) + squaredDistance(r3Diff) + squaredDistance(r4Diff) + squaredDistance(r5Diff)
    
    return ( rankDist + suitDist);
  }
  
  // ================= Mapper function ==============================
  
  // This is the map Function
  def theMapper(line: String) = {
    var trainData: Array[Double] = new Array[Double](11)

    // Split by commas
    val fields = line.split(",")
    //Array to store Suits and Ranks of Current Training Data 
    for(i <- 0 to 9)
      trainData(i) = normalisedDouble(fields(i).toDouble, minSuit, maxSuit)
    // PokerClass
    val pClass = fields(10).toInt
    
    val tDist = totalSquaredDistance(trainData)
    
    (tDist, pClass)
  }
  
 
  //=================== Main function =============================
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "KnnPokerhand")
    // Load each line of the source data into an RDD
    val lines = sc.textFile("../KnnTrainingData.txt")
    
    //Working with testFile
    val testLines = Source.fromFile("../KnnTestingData.txt").getLines()
    
    for (testLine <- testLines)
    {
       var testFields = testLine.split(',')
       for(i <- 0 to 9) {
          testData(i) = normalisedDouble(testFields(i).toDouble, minSuit, maxSuit)
       }
        
        // Use our theMapper function to convert to (Distance, PokerClass) tuples
        val rdd = lines.map(theMapper)
        
        //Sort the rdd elements in an ascending order
        val sortedRdd = rdd.sortByKey()
        
        // Finally take and store top K elements in an array.
        val kNearestNeighbors = sortedRdd.take(K)
        
        var classArr = new Array[Int](K)
        
        //Store the classes in an Array
        for(i <- 0 to (K-1))
          classArr(i) = kNearestNeighbors(i)._2
          
        //Sort and store array of classes 
        val newArr = classArr.sorted
    
        var mostCommonClass = newArr(0)
        var freq = 1
        var currFreq = 1
        var currClass = newArr(0)
        
        //Check for class with highest frequency
        for(i <- 1 to (K-1)) {
           if(currClass == newArr(i)) {
             currFreq = currFreq + 1
           }
           else {
             if(freq < currFreq) {
               mostCommonClass = currClass
               freq = currFreq
             }
             currClass = newArr(i)
             currFreq = 1
           }
        }
        
        if(freq < currFreq) {
          mostCommonClass = currClass
          freq = currFreq
        }
        
        println("Most common class : " + mostCommonClass)
     }
  }
  
}
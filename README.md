# knn

This project focuses on create and compare the performance of MapReduce and spark on a Hadoop Cluster for the same sufficiently large dataset. (which can be found here: https://archive.ics.uci.edu/ml/datasets/Poker+Hand )


# MapReduceCode:

	> Contains the MapReduce code written in Java.

# SparkAppCode:

	> Contains code written in Scala that can be run on a Cluster. 
	Add relevant `hdfs` or `s3` paths for the testing and training data.

	> The app writes the classes of the Test Data to a local `.txt` file on the Master Node.

# AccuracyTest

	> Use `accuracyTest.java` to check the accuracy of the predicted classes. 
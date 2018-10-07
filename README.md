# knn

https://hadoop.apache.org/docs/r2.5.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

This project focuses on creating a MapReduce program for Hadoop so that it can be run for the dataset that can be found here: https://archive.ics.uci.edu/ml/datasets/Poker+Hand

This program will then be used to test and compare the performance of the Hadoop Framework for different physical variables.

Use: "hadoop fs -cat /poker/output/out_*/part-r-00000 > TheCombinedResultOfTheJob.txt" to combine the results of the MR jobs into one txt file for further usage.

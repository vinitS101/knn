#!/bin/sh

#Sample Script

export HADOOP_CLASSPATH=$(hadoop classpath)

#Create Output directory
hadoop fs -mkdir /Output

#Changing directory to location with the Java program and the classes folder.
cd Desktop/poker

javac -classpath ${HADOOP_CLASSPATH} -d "classes" "KnnPokerhand.java"
jar -cvf KNN.jar -C classes/ .

#Finally run the job. Locations are locations of the training and testing data set in HDFS
hadoop jar KNN.jar KnnPokerhand /training_data/train.txt /Output/out /testing_data/test.txt

#Creating txt file with the result for accuracy testing
hadoop fs -cat /Output/out_*/part-r-00000 > KNNOutput.txt
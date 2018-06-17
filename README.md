# Apache-Spark


Spark programs

1.Word count

###############################################################################################################################
Spark-Shell

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext

import org.apache.spark._

val textFile = sc.textFile("hdfs:////inputs/input1.txt")

val counts = textFile.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _)

val textFile = sc.textFile("hdfs:////inputs/input1.txt")

###############################################################################################################################

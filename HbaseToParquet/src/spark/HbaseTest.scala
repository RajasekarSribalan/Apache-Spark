package com.spark.sample

import org.apache.spark.{ SparkConf, SparkContext };
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.StructType
import org.apache.avro.Schema
import com.databricks.spark.avro.SchemaConverters
import org.apache.avro.generic.GenericRecord
import spark.ParquetWriteUtility

object HbaseTest {

  def main(args: Array[String]): Unit = {
    var sparkConfig = new SparkConf().setAppName("Hbase Test")
    sparkConfig.registerKryoClasses(Array(classOf[GenericRecord]))
    var sc: SparkContext = new SparkContext(sparkConfig)
    var hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set(TableInputFormat.INPUT_TABLE, "/employee/");
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc);
    var hBaseRDD = sc.newAPIHadoopRDD(hbaseConfig, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val resultRDD = hBaseRDD.map(tuple => tuple._2)
    resultRDD.count()
    var keyValueRDD = resultRDD.map(result => Deserializer.deserializeRec(result.value))
    var groupedRDD = keyValueRDD.groupByKey();
    groupedRDD.foreach(obj => {
      //  println(obj)
      ParquetWriteUtility.write("/user/spark/" + obj._1, obj._2.toList)
    })
  }

}
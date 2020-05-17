package stream.realtime

import org.apache.spark.sql.SparkSession
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

object HudiExample {
  def main(args: Array[String]): Unit = {
    //Spark Session
    val spark = SparkSession.builder().appName("Kafka-SparkStreaming-Kafka").master("yarn").getOrCreate()

    //Read Stream
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "source-topic")
      .load();

    df.writeStream.foreachBatch((batchDF: DataFrame, batchId: Long) => {

      batchDF.write.format("hudi").
        options(getQuickstartWriteConfigs).
        option(PRECOMBINE_FIELD_OPT_KEY, "timestamp").
        option(RECORDKEY_FIELD_OPT_KEY, "account_id").
        option(PARTITIONPATH_FIELD_OPT_KEY, "account_id").
        option(TABLE_NAME, "hudi_cow").
        option(TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE").
        option(HIVE_SYNC_ENABLED_OPT_KEY, "true").
        option(HIVE_TABLE_OPT_KEY, "hudi_cow").
        option(HIVE_PARTITION_FIELDS_OPT_KEY, "account_id").
        option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName).mode(Append).
        save("/user/raj/job/")

    }).start().awaitTermination();

  }
}
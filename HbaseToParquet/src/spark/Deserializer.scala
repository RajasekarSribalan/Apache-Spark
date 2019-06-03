package com.spark.sample

import java.io.ByteArrayInputStream
import java.io.InputStream

import org.apache.avro.Schema

import org.apache.spark.sql.types.StructType
import com.databricks.spark.avro.SchemaConverters
import org.apache.spark.sql.types.DataType
import org.apache.avro.generic.GenericRecord

object Deserializer {


  def deserializeRec(data: Array[Byte]): (String, GenericRecord) = {

    var inputStream: InputStream = new ByteArrayInputStream(data);
    var binReader = null// TODO Code convert input stream to generic record
    var record: GenericRecord = binReader.read();
    return (record.get("schemaName") + "_" + record.get("schemaVersion"), record);
  }
}
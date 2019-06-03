package com.spark.sample

import org.apache.parquet.hadoop.ParquetWriter
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.hadoop.conf.Configuration
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema

object ParquetWriteUtility {
  def write(pathName: String, records: List[GenericRecord]): Unit = {

    var path: Path = new Path(pathName);
    var writer = AvroParquetWriter.builder[GenericRecord](path).
      withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE).withPageSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
      .withSchema(records(0).getSchema)
      .withConf(new Configuration()).withCompressionCodec(CompressionCodecName.SNAPPY).withWriteMode(Mode.CREATE).build()

    records.foreach(rec => {
      writer.write(rec)
    })
    writer.close();
  }
}
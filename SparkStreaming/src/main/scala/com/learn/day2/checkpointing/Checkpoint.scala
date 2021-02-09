package com.learn.day2.checkpointing

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Checkpoint {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()


        val schema=StructType(List(
          StructField("f1",DataTypes.IntegerType),
          StructField("f2",DataTypes.StringType),
          StructField("f3",DataTypes.IntegerType)
        ))

    val input_df = spark.readStream
      .schema(schema)
      .csv("SparkStreaming/stream_data")


    input_df.writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("csv")
      .option("path", "file_stream_out")
      .option("checkpointLocation", "file_stream_chk")
      .start()
      .awaitTermination()

  }

}

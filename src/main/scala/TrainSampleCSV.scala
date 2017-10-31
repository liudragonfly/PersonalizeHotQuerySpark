import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{avg, udf}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}

import scala.collection.mutable
/**
  * Created by hzliulongfei on 2017/10/30/0030.
  * 生成csv格式训练集
  */
object TrainSampleCSV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val options = new Options()
    options.addOption("train_sample", true, "train file input")
    options.addOption("output", true, "output")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val trainSampleInput = cmd.getOptionValue("train_sample")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val trainSampleSchema = StructType(Seq(
      StructField("uid", StringType, true),
      StructField("queryid", StringType, true),
      StructField("label", StringType, true)
    ))

    val trainSampleUDF = udf((row: String) => {
      val trainSampleArray = new Array[String](3)
      val array = row.split("\t")
      val uid = array(0)
      val queryid = array(1)
      val label = array(3)

      array(0) = uid
      array(1) = queryid
      array(2) = label

      Row.fromSeq(trainSampleArray)
    })

    val trainSampleDF = spark.read.textFile(trainSampleInput).withColumn("info", trainSampleUDF($"value"))
    .select($"info.uid", $"info.queryid", $"info.label").na.drop()
    trainSampleDF.printSchema()

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if(fileSystem.exists(new Path(output))) fileSystem.delete(new Path(output), true)

    trainSampleDF.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(output)
  }
}

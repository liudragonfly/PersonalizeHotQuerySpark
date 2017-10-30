import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{avg, udf}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}

import scala.collection.mutable
import scala.util.control.Breaks

/**
  * Created by hzliulongfei on 2017/10/30/0030.
  */
object UserProfileFeatureCSV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val options = new Options()
    options.addOption("user_profile", true, "user profile input")
    options.addOption("output", true, "output")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val userProfileInput = cmd.getOptionValue("user_profile")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    // 用户性别
    val userProfileSchema = StructType(Seq(
      StructField("uid", StringType, true),
      StructField("gender", StringType, true)
    ))

    val userProfileUDF = udf((row:String) => {
      val userProfileArray = new Array[String](2)
      val array = row.split("\t")
      val uid = array(0)
      val gender = array(2) match {
        case "0" => "0"
        case "1" => "1"
        case "2" => "2"
        case _ => "0"
      }
      userProfileArray(0) = uid
      userProfileArray(1) = gender
      Row.fromSeq(userProfileArray)
    }, userProfileSchema)

    val userProfileDF = spark.read.textFile(userProfileInput).withColumn("info", userProfileUDF($"value"))
    .select($"info.uid", $"info.gender")
    userProfileDF.printSchema()

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if(fileSystem.exists(new Path(output))) fileSystem.delete(new Path(output), true)

    userProfileDF.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(output)
  }
}

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
object UserClientFeatureCSV {
  def main(args: Array[String]) : Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val options = new Options()
    options.addOption("user_client", true, "user client input")
    options.addOption("output", true, "output")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val userClientInput = cmd.getOptionValue("user_client")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    // 用户客户端
    val userClientSchema = StructType(Seq(
      StructField("uid", StringType, true),
      StructField("client", StringType, true)
    ))

    val userClientUDF = udf((row: String) => {
      // 因为个性化热搜只在android和iphone有 故只按照顺序取这两个客户端 如果没有iphone或android则用other表示
      // 110860866	pc:0.51724136,android:0.4827586
      val userClientArray = new Array[String](2)
      val array = row.split("\t")
      val uid = array(0)

      val clientsStr = array(1)
      val clients = clientsStr.split(",")
      var client = "other"
      val loop = new Breaks
      loop.breakable{
        for(each <- clients){
          if(each.startsWith("iphone")){
            client = "iphone"
            loop.break
          }else if(each.startsWith("android")){
            client = "android"
            loop.break
          }
        }
      }
      userClientArray(0) = uid
      userClientArray(1) = client
      Row.fromSeq(userClientArray)
    }, userClientSchema)

    val userClientDF = spark.read.textFile(userClientInput).withColumn("info", userClientUDF($"value"))
    .select($"info.uid", $"info.client")
    userClientDF.printSchema()

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if(fileSystem.exists(new Path(output))) fileSystem.delete(new Path(output), true)

    userClientDF.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(output)
  }
}

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{avg, udf}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}

import scala.collection.mutable

/**
  * Created by hzliulongfei on 2017/10/31/0031.
  * 对样本关联特征
  */
object SampleWithFeatureCSV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val options = new Options()
    options.addOption("sample", true, "sample input")
    options.addOption("user_client", true, "user client input")
    options.addOption("user_profile", true, "user profile input")
    options.addOption("query_useraction", true, "query user action input")
    options.addOption("output", true, "output")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val sampleInput = cmd.getOptionValue("sample")
    val userClientInput = cmd.getOptionValue("user_client")
    val userProfileInput = cmd.getOptionValue("user_profile")
    val queryUserActionInput = cmd.getOptionValue("query_useraction")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    spark.read.option("header", "true").csv(sampleInput).createOrReplaceTempView("sample")
    spark.read.option("header", "true").csv(userClientInput).createOrReplaceTempView("user_client")
    spark.read.option("header", "true").csv(userProfileInput).createOrReplaceTempView(("user_profile"))
    spark.read.option("header", "true").csv(queryUserActionInput).createOrReplaceTempView("query_useraction")
    // TODO: 多个表进行join
    val sampleWithFeatureDF = spark.sql(
      """select sample.uid, sample.queryid, sample.label, user_client.client, user_profile.gender,
        | query_useraction.total, query_useraction.click, query_useraction.ps_count, query_useraction.add,
        | query_useraction.download, query_useraction.like, query_useraction.ppdown, query_useraction.pplike
        | from (
        | (
        | sample left join user_client
        | on sample.uid=user_client.uid
        | ) t1 left join user_profile
        | on t1.uid=user_profile.uid
        | )t2 left join query_useraction
        | on t2.queryid=query_useraction.queryid
      """.stripMargin)

    sampleWithFeatureDF.printSchema()

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if(fileSystem.exists(new Path(output))) fileSystem.delete(new Path(output), true)

    sampleWithFeatureDF.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(output)

    spark.stop()

  }
}

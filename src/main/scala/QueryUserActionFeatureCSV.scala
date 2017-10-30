import org.apache.spark.sql.SparkSession
import org.apache.commons.cli.{Options, PosixParser}
import util.{HashFunc, RecentDate}

import scala.collection.JavaConversions._
import org.apache.spark.sql.functions.{avg, udf}
import org.apache.hadoop.fs.{FileSystem, Path}
/**
  * Created by hzliulongfei on 2017/10/12/0012.
  * 抽取最近N天query级别的用户行为特征
  */
object QueryUserActionFeatureCSV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val options = new Options()
    options.addOption("input", true, "input dir")
    options.addOption("output", true, "output dir")
    options.addOption("start_date", true, "start date")
    options.addOption("ndays_before", true, "n days before")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val input = cmd.getOptionValue("input")
    val output = cmd.getOptionValue("output")
    val startDate = cmd.getOptionValue("start_date")
    val nDaysBefore = cmd.getOptionValue("ndays_before").toInt

    val dates = RecentDate.getDateNDaysBefore(startDate, nDaysBefore)
    val paths = dates.map(date => input + "/" + date)

    val pathsStr = paths.mkString(",")
    println(pathsStr)

    import spark.implicits._

    // josn(String*) 需要将Seq进行解包操作 paths: _*就是解包操作
    val infoOriginalDF = spark.read.json(paths: _*)
    infoOriginalDF.createOrReplaceTempView("info_original")

    val infoDF = spark.sql("select id, q, anttypes.total, anttypes.click, anttypes.ps_count, anttypes.add, anttypes.download, anttypes.like, anttypes.ppdown, anttypes.pplike from info_original")
    infoDF.printSchema()

    val infoNoNaDF = infoDF.na.fill(0, Seq("total", "click", "ps_count", "add", "download", "like", "ppdown", "pplike"))

    val infoAvgDF = infoNoNaDF.groupBy($"q",$"id").agg(avg($"total").alias("total"), avg($"click").alias("click"), avg($"ps_count").alias("ps_count"),
      avg($"add").alias("add"),avg($"download").alias("download"),avg($"like").alias("like"), avg($"ppdown").alias("ppdown"), avg($"pplike").alias("pplike"))
    infoAvgDF.printSchema()

    infoAvgDF.createOrReplaceTempView("info_avg")
    val infoMaxTotalDF = spark.sql(
      """select * from(
        |select id, q, total, click, ps_count, add, download, like, ppdown, pplike, row_number() over (partition by q order by total desc) rank from info_avg
        |) rk
        |where rk.rank=1 order by total desc
      """.stripMargin)

    infoMaxTotalDF.printSchema()

     val queryidUDF = udf((query: String) => {
       val queryid = HashFunc.BKDRHash(query.toLowerCase)
       queryid
     })

    val resultDF = infoMaxTotalDF.withColumn("queryid", queryidUDF($"q"))

    resultDF.printSchema()
    resultDF.show()

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if(fileSystem.exists(new Path(output))) fileSystem.delete(new Path(output), true)

    resultDF.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(output)

    spark.stop()
  }
}

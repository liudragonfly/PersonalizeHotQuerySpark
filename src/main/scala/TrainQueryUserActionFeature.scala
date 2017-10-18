import org.apache.spark.sql.SparkSession
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.functions.{avg, udf}
import org.apache.hadoop.fs.{FileSystem, Path}
/**
  * Created by hzliulongfei on 2017/10/14/0014.
  */
object TrainQueryUserActionFeature {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val options = new Options()
    options.addOption("train", true, "train")
    options.addOption("query_user_action", true, "query user action")
    options.addOption("output", true, "output")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val trainInput = cmd.getOptionValue("train")
    val queryUserActionInput = cmd.getOptionValue("query_user_action")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val extractQueryidUDF = udf((row: String) => {
      val queryid = row.split("\t")(1)
      queryid
    })

    val queryidDF = spark.read.textFile(trainInput).withColumn("queryid", extractQueryidUDF($"value")).select($"queryid").distinct()
    queryidDF.printSchema()
    queryidDF.createOrReplaceTempView("queryid_distinct")

    val queryUserAcionDF = spark.read.option("header", "true").csv(queryUserActionInput)
    queryUserAcionDF.printSchema()
    queryUserAcionDF.createOrReplaceTempView("query_user_action")

    val resultDF = spark.sql(
      """select t1.queryid, t2.total, t2.click, t2.ps_count, t2.add, t2.download, t2.like, t2.ppdown, t2.pplike from
        |queryid_distinct t1
        |left join query_user_action t2
        |on t1.queryid = t2.queryid
      """.stripMargin).na.fill("0", Seq("total", "click", "ps_count", "add", "download", "like", "ppdown", "pplike"))

    resultDF.printSchema()
    resultDF.show()

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if(fileSystem.exists(new Path(output))) fileSystem.delete(new Path(output), true)

    resultDF.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(output)

    spark.stop()
  }
}

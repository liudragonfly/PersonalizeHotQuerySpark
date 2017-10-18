import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions.{avg, udf}
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.control._

/**
  * Created by hzliulongfei on 2017/10/13/0013.
  * 生成用户特征
  */
object TrainUserClientFeature {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val options = new Options()
    options.addOption("train", true, "train file input")
    options.addOption("user_client", true, "user client input")
    // other feature input

    options.addOption("output", true, "output")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val trainInput = cmd.getOptionValue("train")
    val userClientInput = cmd.getOptionValue("user_client")
    val output = cmd.getOptionValue("output")

    import spark.implicits._
    val extractUidUDF = udf((row: String) => {
      val uid = row.split("\t")(0)
      uid
    })

    // 因为个性化热搜只在android和iphone有 故只按照顺序取这两个客户端 如果没有iphone或android则用other表示
    // 110860866	pc:0.51724136,android:0.4827586
    val extractClientUDF = udf((row: String) => {
      val clientsStr = row.toLowerCase.split("\t")(1)
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
      client
    })

    // trainInput是HDFS text文件 需要将其转成dataframe
    val uidDF = spark.read.textFile(trainInput).withColumn("uid", extractUidUDF($"value"))
    uidDF.printSchema()

    val uidDistinctDF = uidDF.select($"uid").distinct()
    uidDistinctDF.printSchema()
    uidDistinctDF.createOrReplaceTempView("uid_distinct")

    // userClientInput是HDFS text文件 需要将其转成dataframe
    val userClientDF = spark.read.textFile(userClientInput).withColumn("uid", extractUidUDF($"value")).withColumn("client", extractClientUDF($"value")).na.fill("other", Seq("client"))
    userClientDF.printSchema()
    userClientDF.createOrReplaceTempView("user_client")

    // 将trainDF和userClientDF进行join
    val resultDF = spark.sql("select t1.uid, t2.client from uid_distinct t1 left join user_client t2 on t1.uid = t2.uid").na.fill("other", Seq("client"))
    resultDF.printSchema()
    resultDF.show()

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if(fileSystem.exists(new Path(output))) fileSystem.delete(new Path(output), true)

    resultDF.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(output)

    spark.stop()
  }
}

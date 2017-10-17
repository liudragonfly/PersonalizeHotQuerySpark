import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{avg, udf}
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by hzliulongfei on 2017/10/17/0017.
  */
object TrainUserGenderFeature {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val options = new Options()
    options.addOption("train", true, "train file input")
    options.addOption("user_profile", true, "user profile input")
    options.addOption("output", true, "output")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val trainInput = cmd.getOptionValue("train")
    val userProfileInput = cmd.getOptionValue("user_profile")
    val output = cmd.getOptionValue("output")

    import spark.implicits._
    val extractUidUDF = udf((row: String) => {
      // UserId \t LastLoginTime \t Gender \t City \t AccountStatus \t Birthday \t Province \t CreateTime
      val uid = row.split("\t")(0)
      uid
    })

    val extractGenderUDF = udf((row: String) => {
      // UserId \t LastLoginTime \t Gender \t City \t AccountStatus \t Birthday \t Province \t CreateTime
      val gender = row.split("\t")(2)
      gender
    })

    val uidDistinctDF = spark.read.textFile(trainInput).withColumn("uid", extractUidUDF($"value")).select($"uid").distinct()
    uidDistinctDF.printSchema()
    uidDistinctDF.createOrReplaceTempView("uid_distinct")

    val userGenderDF = spark.read.textFile(userProfileInput).withColumn("uid", extractUidUDF($"value")).withColumn("gender", extractGenderUDF($"value")).select($"uid", $"gender")
    userGenderDF.printSchema()
    userGenderDF.createOrReplaceTempView("user_gender")

    val resultDF = spark.sql(
      """select t2.* from
        |uid_distinct t1 left join user_gender t2
        |on t1.uid = t2.uid
      """.stripMargin)

    resultDF.printSchema()
    resultDF.show()

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if(fileSystem.exists(new Path(output))) fileSystem.delete(new Path(output), true)

    spark.stop()
  }
}

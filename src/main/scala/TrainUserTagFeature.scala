import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, udf}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}

import scala.collection.mutable
import scala.util.control._

/**
  * Created by hzliulongfei on 2017/10/14/0014.
  * 抽取用户的tag特征
  */
object TrainUserTagFeature {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val options = new Options
    options.addOption("train", true, "train file input")
    options.addOption("user_tag", true, "user tag input")

    options.addOption("output", true, "output")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val trainInput = cmd.getOptionValue("train")
    val userTagInput = cmd.getOptionValue("user_tag")
    val output = cmd.getOptionValue("output")

    import spark.implicits._
    val extractUidUDF = udf((row: String) => {
      val uid = row.split("\t")(0)
      uid
    })

    val tagIndex = mutable.HashMap(
      "摇滚_7" -> 0,
      "民谣_7" -> 1,
      "说唱_7" -> 2,
      "轻音乐_7" -> 3,
      "古风_7" -> 4,
      "影视原声_7" -> 5,
      "轻音乐_8" -> 6,
      "ACG_8" -> 7,
      "影视原声_8" -> 8,
      "民谣_16" -> 9,
      "轻音乐_16" -> 10,
      "影视原声_16" -> 11,
      "摇滚_96" -> 12,
      "民谣_96" -> 13,
      "电子_96" -> 14,
      "说唱_96" -> 15,
      "轻音乐_96" -> 16,
      "爵士_96" -> 17,
      "影视原声_96" -> 18,
      "乡村_96" -> 19,
      "R&B/Soul_96" -> 20,
      "金属_96" -> 21,
      "朋克_96" ->22,
      "雷鬼_96" -> 23,
      "拉丁_96" -> 24,
      "摇滚_1073741824" -> 25,
      "民谣_1073741824" -> 26,
      "电子_1073741824" -> 27,
      "说唱_1073741824" -> 28,
      "轻音乐_1073741824" -> 29,
      "爵士_1073741824" -> 30,
      "影视原声_1073741824" -> 31,
      "乡村_1073741824" -> 32,
      "R&B/Soul_1073741824" -> 33,
      "金属_1073741824" -> 34,
      "朋克_1073741824" -> 35,
      "雷鬼_1073741824" -> 36,
      "拉丁_1073741824" -> 37
    )
    val tagSchema = StructType(Seq(
      StructField("摇滚_7", FloatType, false),
      StructField("民谣_7", FloatType, false),
      StructField("说唱_7", FloatType, false),
      StructField("轻音乐_7", FloatType, false),
      StructField("古风_7", FloatType, false),
      StructField("影视原声_7", FloatType, false),
      StructField("轻音乐_8", FloatType, false),
      StructField("ACG_8", FloatType, false),
      StructField("影视原声_8", FloatType, false),
      StructField("民谣_16", FloatType, false),
      StructField("轻音乐_16", FloatType, false),
      StructField("影视原声_16", FloatType, false),
      StructField("摇滚_96", FloatType, false),
      StructField("民谣_96", FloatType, false),
      StructField("电子_96", FloatType, false),
      StructField("说唱_96", FloatType, false),
      StructField("轻音乐_96", FloatType, false),
      StructField("爵士_96", FloatType, false),
      StructField("影视原声_96", FloatType, false),
      StructField("乡村_96", FloatType, false),
      StructField("R&B/Soul_96", FloatType, false),
      StructField("金属_96", FloatType, false),
      StructField("朋克_96", FloatType, false),
      StructField("雷鬼_96", FloatType, false),
      StructField("拉丁_96", FloatType, false),
      StructField("摇滚_1073741824", FloatType, false),
      StructField("民谣_1073741824", FloatType, false),
      StructField("电子_1073741824", FloatType, false),
      StructField("说唱_1073741824", FloatType, false),
      StructField("轻音乐_1073741824", FloatType, false),
      StructField("爵士_1073741824", FloatType, false),
      StructField("影视原声_1073741824", FloatType, false),
      StructField("乡村_1073741824", FloatType, false),
      StructField("R&B/Soul_1073741824", FloatType, false),
      StructField("金属_1073741824", FloatType, false),
      StructField("朋克_1073741824", FloatType, false),
      StructField("雷鬼_1073741824", FloatType, false),
      StructField("拉丁_1073741824", FloatType, false)
    ))

    // 抽取用户tag特征
    val extractTagUDF = udf((row: String) => {
      val tagScoreArray = new Array[Float](38)
      val array = row.split("\t")
      val userid = array(0)
      val tagsArray = array(1).split(",")
      for(tagStr <- tagsArray){
        val tagArray = tagStr.split("\t")
        val tag = tagArray(0)
        val score = tagArray(1).toFloat
        val index = tagIndex.getOrElse(tag, -1)
        if(index >= 0){
          tagScoreArray(index) = score
        }
      }
      tagScoreArray
    }, tagSchema)

    val uidDistinctDF = spark.read.textFile(trainInput).withColumn("uid", extractUidUDF($"value")).select($"uid").distinct()
    uidDistinctDF.printSchema()
    uidDistinctDF.createOrReplaceTempView("user_distinct")

    val userTagDF = spark.read.textFile(userTagInput).withColumn("queryid", extractUidUDF($"value")).withColumn("tag", extractTagUDF($"value"))
    userTagDF.printSchema()

    // 将trainDF和userTagDF进行join

    spark.stop()
  }
}

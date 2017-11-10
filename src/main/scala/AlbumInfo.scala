import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import java.util.{Calendar, Date}
/**
  * Created by hzliulongfei on 2017/8/31/0031.
  * 抽取专辑信息
  */
object AlbumInfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val options = new Options()
    options.addOption("input", true, "input dir")
    options.addOption("output", true, "output dir")
    options.addOption("recent_days", true, "recent days")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val input = cmd.getOptionValue("input")
    val output = cmd.getOptionValue("output")
    val recentDays = cmd.getOptionValue("recent_days").toInt

    val musicAlbumDF = spark.read.parquet(input)
    musicAlbumDF.createOrReplaceTempView("music_album")

    val nowDate = new Date()
    val nowTime = nowDate.getTime

    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -recentDays)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    val recentTime = calendar.getTimeInMillis


    val resultDF = spark.sql(
      s"""SELECT id, artistid, name, artistname, artists, score, fee
         |FROM music_album
         |WHERE id>0
         |AND valid=99
         |AND publishtime>=$recentTime
         |AND publishtime<=$nowTime
      """.stripMargin
    )

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if(fileSystem.exists(new Path(output))) fileSystem.delete(new Path(output), true)

    resultDF.coalesce(1).write.json(output)
    spark.stop()
  }
}

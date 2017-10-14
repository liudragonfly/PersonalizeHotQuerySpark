import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
/**
  * Created by hzliulongfei on 2017/7/27/0027.
  * 抽取出艺人及其别名信息
  */
object ArtistNameAlias {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val options = new Options()
    options.addOption("input", true, "input dir")
    options.addOption("output", true, "output dir")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val input = cmd.getOptionValue("input")
    val output = cmd.getOptionValue("output")

    val dataDF = spark.read.json(input)
    dataDF.createOrReplaceTempView("artist_table")
    val resultDF = spark.sql("select dataFields.ID, dataFields.Name, dataFields.AliasKeyword from artist_table where op='ADD' and dataFields.Valid=99")

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if(fileSystem.exists(new Path(output))) fileSystem.delete(new Path(output), true)

    resultDF.coalesce(1).write.json(output)
    spark.stop()
  }
}

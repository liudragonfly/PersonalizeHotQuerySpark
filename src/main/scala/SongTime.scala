import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by hzliulongfei on 2017/8/10/0010.
  * 抽取出歌曲的时长信息 用于计算用户点击播放的歌曲播放率
  * 利用song1的时长300s 用户点击播放了150s 那么歌曲播放率为0.5
  */
object SongTime {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val options = new Options()
    options.addOption("music_song", true, "music_song input dir")
    options.addOption("music_newmusic", true, "music_newmusic input dir")
    options.addOption("output", true, "output dir")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val musicSongPath = cmd.getOptionValue("music_song")
    val musicNewMusicPath = cmd.getOptionValue("music_newmusic")
    val outputPath = cmd.getOptionValue("output")

    val musicSongDF = spark.read.parquet(musicSongPath)
    musicSongDF.createOrReplaceTempView("music_song_table")

    val musicNewMusicDF = spark.read.parquet(musicNewMusicPath)
    musicNewMusicDF.createOrReplaceTempView("music_newmusic_table")

    val resultDF = spark.sql(
      """SELECT music_song_valid_table.id, music_newmusic_table.time
        |FROM
        |(
        |SELECT music_song_table.id, music_song_table.musicid FROM music_song_table
        |WHERE music_song_table.valid=99
        |AND music_song_table.albumid>0
        |AND music_song_table.id>0
        |) music_song_valid_table
        | JOIN music_newmusic_table
        | ON music_song_valid_table.musicid = music_newmusic_table.id
      """.stripMargin)

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if(fileSystem.exists(new Path(outputPath))) fileSystem.delete(new Path(outputPath), true)

    resultDF.coalesce(32).write.json(outputPath)
    spark.stop()
  }
}

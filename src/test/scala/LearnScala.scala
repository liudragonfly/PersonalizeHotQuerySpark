import java.util.{Calendar, Date}

import util.RecentDate

import scala.collection.JavaConversions._
/**
  * Created by hzliulongfei on 2017/8/31/0031.
  */
object LearnScala {
  def main(args: Array[String]): Unit = {
//    val date = new Date()
//    val calendar = Calendar.getInstance()
//    calendar.add(Calendar.DATE, -1)
//    calendar.set(Calendar.HOUR_OF_DAY, 0)
//    calendar.set(Calendar.MINUTE, 0)
//    calendar.set(Calendar.SECOND, 0)
//    calendar.set(Calendar.MILLISECOND, 0)
//    println(calendar.getTimeInMillis, date.getTime)
//    println(s"""SELECT id, artistid, name, artistname, artists
//                     |FROM music_album
//                     |WHERE id>0
//                     |AND status>=0
//                     |AND valid=99
//                     |AND publishtime>=${calendar.getTimeInMillis}
//                     |AND publishtime<=${date.getTime}
//      """.stripMargin)
    val dates = RecentDate.getDateNDaysBefore("2017-10-11", 7)
    val paths = dates.map(s => "/usr/" + s)
    println(paths.mkString(","))
  }
}

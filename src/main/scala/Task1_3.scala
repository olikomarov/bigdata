import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

//count of videos, audios, photos, gifts

object Task1_3 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    println("count of videos, audios, photos, gifts")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()


    val fol_profiles = spark.read.parquet(s"D:\\bigdata_source\\followerProfiles.parquet")
    val profiles_filt = fol_profiles.filter(fol_profiles("counters").isNotNull)

    val cols = Seq("id","counters")
    val data1 = profiles_filt.select(cols.head, cols.tail: _*)
    val data2 = profiles_filt.select(cols.head, cols.tail: _*).select(data1("id"), data1("counters.videos"), data1("counters.audios"),
      data1("counters.photos"),data1("counters.gifts"))

    val stat = Seq("from_id", "videos", "audios", "photos", "gifts")
    val stat_fin = data2.toDF(stat: _*)
    stat_fin.show(10)
    stat_fin.write.parquet("task1_3.parquet")
  }
}
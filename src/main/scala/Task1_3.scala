import org.apache.spark.sql.SparkSession

import org.apache.log4j.Logger
import org.apache.log4j.Level

//count of videos, audios, photos, gifts

object Task1_3 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    println("count of videos, audios, photos, gifts")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()


    val fol_profiles = spark.read.parquet(s"D:\\bigdata_source\\followerProfiles.parquet")
    val profiles_filt = fol_profiles.filter(fol_profiles("counters").isNotNull)

    val cols = Seq("id","counters")

    val red = profiles_filt.select(cols.head, cols.tail: _*)

    val ids = red.select(red("id"), red("counters.videos"), red("counters.audios"),
      red("counters.photos"),red("counters.gifts"))

    val df = Seq("from_id", "videos", "audios", "photos", "gifts")
    val df_fin = ids.toDF(df: _*)
    df_fin.show(10)
    df_fin.write.parquet("task1_3.parquet")
  }
}
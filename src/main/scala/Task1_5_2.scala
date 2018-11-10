import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//count of "incoming" likes, max and mean "incoming" likes per post

object Task1_5_2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    println("max and mean incoming likes per post")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()

    val likes_res = spark.read.parquet("task1_5_1.parquet")

    val max_mean = likes_res.groupBy("ownerId")
      .agg(
        max("incoming_likes").alias("incoming_likes_max"),
        mean("incoming_likes").alias("incoming_likes_mean")
      )
    val col = Seq("from_id", "incoming_likes_num", "incoming_likes_mean")
    val likes_mm = max_mean.toDF(col: _*)
    likes_mm.show(10)
    likes_mm.write.parquet("task1_5_2.parquet")

  }
}
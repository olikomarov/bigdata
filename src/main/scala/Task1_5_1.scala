import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger

//count of "incoming" likes, max and mean "incoming" likes per post

object Task1_5_1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    println("count of incoming likes per post")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()

    val likes = spark.read.parquet(s"D:\\bigdata_source\\userWallLikes.parquet")
    val likes_incoming = likes.filter(likes("likerId") =!= likes("ownerId"))

    val likes_fin = likes_incoming.groupBy( "ownerId", "itemId")
      .agg(count("key").alias("incoming_likes"))
    likes_fin.show(10)
    likes_fin.write.parquet("task1_5_1.parquet")
  }
}
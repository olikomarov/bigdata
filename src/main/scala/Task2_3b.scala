import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger
//aggregate (e.g. count, max, mean) characteristics for comments and likes (separately) per post

object Task2_3b {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    println("Hello scala")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()

    val friends = spark.read.parquet(s"D:\\bigdata_source\\friends.parquet")
    val likes = spark.read.parquet(s"D:\\bigdata_source\\userWallLikes.parquet")

    val likes_from_users = likes
      .filter("likerId > 0")
      .groupBy(col("itemId"), col("ownerId"), col("likerId"))
      .count()

    val likes_stat = friends
      .join(likes_from_users,
        likes_from_users("likerId") === friends("follower").cast("int") &&
          likes_from_users("ownerId") === friends("profile").cast("int"))
      .groupBy("profile")
      .agg(
        sum("count").alias("friends_likes"),
        max("count").alias("friends_likes_max"),
        mean("count").alias("friends_likes_mean")
      )
      .withColumnRenamed("profile", "from_id")

    likes_stat.show(10)
    likes_stat.write.parquet("task2_3b.parquet")
  }
}
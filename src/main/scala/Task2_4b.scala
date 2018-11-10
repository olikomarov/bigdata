import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger

//aggregate (e.g. count, max, mean) characteristics for comments and likes (separately) per user

object Task2_4b{
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    println("aggregate likes per user")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()


    val likes = spark.read.parquet(s"D:\\bigdata_source\\userWallLikes.parquet")
    val followers = spark.read.parquet(s"D:\\bigdata_source\\followers.parquet")

    val likes_from_users = likes
      .filter("likerId > 0")
      .groupBy(col("itemId"), col("ownerId"), col("likerId"))
      .count()

    val likes_stat = followers
      .join(likes_from_users,
        likes_from_users("likerId") === followers("follower").cast("int") &&
          likes_from_users("ownerId") === followers("profile").cast("int"))
      .groupBy("profile")
      .agg(
        sum("count").alias("followers_likes"),
        max("count").alias("followers_likes_max"),
        mean("count").alias("followers_likes_mean")
      )
      .withColumnRenamed("profile", "from_id")

    likes_stat.show(10)
    likes_stat.write.parquet("test2_4b.parquet")
  }
}
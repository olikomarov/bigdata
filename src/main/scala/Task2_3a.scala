import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger

//aggregate (e.g. count, max, mean) characteristics for comments and likes (separately) per post

object Task2_3a {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    println("aggregate characteristics for comments")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()

    val friends = spark.read.parquet(s"D:\\bigdata_source\\friends.parquet")
    val comments = spark.read.parquet(s"D:\\bigdata_source\\userWallComments.parquet")
    val comments_from_users = comments
      .filter("from_id > 0")
      .groupBy(col("id"), col("post_owner_id"), col("from_id"))
      .count()

    val comments_stat = friends
      .join(comments_from_users,
        comments_from_users("from_id") === friends("follower").cast("int") &&
          comments_from_users("post_owner_id") === friends("profile").cast("int"))
      .groupBy("profile")
      .agg(
        sum("count").alias("comments"),
        max("count").alias("comments_max"),
        mean("count").alias("comments_mean")
      )
      .withColumnRenamed("profile", "from_id")

    comments_stat.show(10)
    comments_stat.write.parquet("task2_3a.parquet")
  }
}
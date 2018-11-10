import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

//count of reposts from subscribed and not-subscribed groups

object Task2_1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    println("count of reposts from subscribed and not-subscribed groups")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()

    val posts = spark.read.parquet(s"D:\\bigdata_source\\userWallPosts.parquet")

    val posts_filt = posts
      .filter(posts("is_reposted"))
      .filter(posts("repost_info.orig_owner_id") < 0)
      .withColumn("tmp", concat(col("from_id"), lit("_"), col("repost_info.orig_owner_id")))

    val user_groups = spark.read.parquet(s"D:\\bigdata_source\\userGroupsSubs.parquet")
    val user_gr_new =  user_groups.withColumnRenamed("key", "tmp")

    val with_sub_reposts = posts_filt.join(user_gr_new, Seq("tmp"))
       .groupBy("from_id")
       .agg(count("from_id").alias("reposts_sub_num"))

    with_sub_reposts.show(10)
    with_sub_reposts.write.parquet("task2_1_a.parquet")

    val without_sub_reposts = posts_filt.join(user_gr_new, Seq("tmp"), "leftanti")
      .groupBy("from_id")
      .agg(count("from_id").alias("reposts_sub_num"))

    without_sub_reposts.show(10)
    without_sub_reposts.write.parquet("task2_1_b.parquet")

  }
}
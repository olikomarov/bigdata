import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

//count of geo tagged posts

object Task1_6 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    println("count of geo tagged posts")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()

    val posts = spark.read.parquet(s"D:\\bigdata_source\\userWallPosts.parquet")

    val all_geo_posts = posts.groupBy("from_id").count()
    val all_posts_names = Seq("from_id", "all_geo_posts_count")

    val all_geo_posts_df= all_geo_posts.toDF(all_posts_names: _*)
    all_geo_posts_df.show(10)

    all_geo_posts_df.write.parquet("task1_6.parquet")
  }
}
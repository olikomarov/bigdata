import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

//1) count of comments, posts (all), original posts, reposts and likes made by user

object Task1_1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    println("count of comments, posts (all), original posts, reposts")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()

    //Posts
    val posts = spark.read.parquet(s"D:\\bigdata_source\\userWallPosts.parquet")
    val all_posts = posts.groupBy("from_id").count()
    val all_posts_names = Seq("from_id", "all_posts_count")
    val original_posts = posts.filter(posts("from_id") === posts("owner_id")).groupBy("from_id").count()
    val original_posts_names = Seq("from_id", "original_posts_count")

    val all_posts_df = all_posts.toDF(all_posts_names: _*)
    val original_posts_df = original_posts.toDF(original_posts_names: _*)
    val merged = all_posts_df.join(original_posts_df, Seq("from_id"))

    //Comments

    val comments = spark.read.parquet(s"D:\\bigdata_source\\userWallComments.parquet")
    val all_comments = comments.groupBy("post_owner_id").count()
    val all_comments_names = Seq("from_id", "all_comments_count")
    val all_comments_df = all_comments.toDF(all_comments_names: _*)

    val merged2 = merged.join(all_comments_df, Seq("from_id"))

    //likes

    val likes = spark.read.parquet(s"D:\\bigdata_source\\userWallLikes.parquet")
    val all_likes = likes.groupBy("likerId").count()
    val all_likes_names = Seq("from_id", "all_likes_count")
    val all_likes_df = all_likes.toDF(all_likes_names: _*)

    val merged_fin = merged2.join(all_likes_df, Seq("from_id"))

    merged_fin.show(20)
    merged_fin.write.parquet("task1_1.parquet")
  }
}
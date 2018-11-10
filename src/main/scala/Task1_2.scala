import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

//2.count of friends, groups, followers
object Task1_2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    println("count of friends, groups, followers")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()

    //friends
    val friends = spark.read.parquet(s"D:\\bigdata_source\\friends.parquet")
    val friends_num = friends.groupBy("profile").count()
    val all_friends_names = Seq("from_id", "all_friends_count")
    val all_friends_df = friends_num.toDF(all_friends_names: _*)

    //groups
    val groups = spark.read.parquet(s"D:\\bigdata_source\\userGroupsSubs.parquet")
    val groups_num = groups.groupBy("user").count()
    val all_groups_names = Seq("from_id", "all_groups_count")
    val all_groups_df = groups_num.toDF(all_groups_names: _*)

    //followers
    val followers = spark.read.parquet(s"D:\\bigdata_source\\followers.parquet")
    val followers_num = followers.groupBy("profile").count()
    val all_followers_names = Seq("from_id", "all_followers_count")
    val all_followers_df = followers_num.toDF(all_followers_names: _*)

    val merged = all_followers_df.join(all_friends_df, Seq("from_id"))
    val merged2 = merged.join(all_groups_df, Seq("from_id"))
    merged2.show(10)

    merged2.write.parquet("task1_2.parquet")
  }
}
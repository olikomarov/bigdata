import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

//count of deleted users in friends and followers

object Task2_2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    println("count of deleted users in friends and followers")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()

    val friends_profiles = spark.read.parquet(s"D:\\bigdata_source\\friendsProfiles.parquet")
    val friends = spark.read.parquet(s"D:\\bigdata_source\\friends.parquet")

    var deact_filt = friends_profiles.filter(friends_profiles("deactivated").isNull)
    var deact_friends = friends.join(deact_filt, deact_filt("id") === friends("profile")).groupBy("profile").count()

    var act_filt = friends_profiles.filter(friends_profiles("deactivated").isNotNull)
    var act_friends = friends.join(act_filt, act_filt("id") === friends("profile")).groupBy("profile").count()

    val merged = act_friends.join(deact_friends, Seq("profile"))
    val col = Seq("from_id", "act_friends", "deact_friends")
    val friends_res = merged.toDF(col: _*)
    friends_res.show(10)
   friends_res.write.parquet("task2_2.parquet")

  }
}
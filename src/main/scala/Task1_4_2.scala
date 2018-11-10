import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger

//count of "incoming" (made by other users) comments, max and mean "incoming" comments per post

object Task1_4_2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    println("max and mean incoming comments per post")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()

    val comments = spark.read.parquet(s"D:\\bigdata_source\\userWallComments.parquet")
    val com_incoming = comments.filter(comments("from_id") =!= comments("post_owner_id"))

    val com_incoming_cnt = com_incoming.groupBy( "post_owner_id", "post_id")
      .agg(count("id").alias("incoming_comments_num"))


    val incoming_max_mean = com_incoming_cnt.groupBy("post_owner_id")
      .agg(max("incoming_comments_num").alias("incoming_comments_max"),
        mean("incoming_comments_num").alias("incoming_comments_mean"))

    val cols = Seq("from_id", "incoming_comments_max", "incoming_comments_mean")
    val max_mean_fin = incoming_max_mean.toDF(cols: _*)

    max_mean_fin.show(10)
    max_mean_fin.write.parquet("task1_4_2.parquet")

  }
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger

//count of "incoming" (made by other users) comments, max and mean "incoming" comments per post

object Task1_4_1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    println("count of incoming (made by other users) comments")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()

    val comments = spark.read.parquet(s"D:\\bigdata_source\\userWallComments.parquet")
    val com_incoming = comments.filter(comments("from_id") =!= comments("post_owner_id"))

    val com_incoming_num = com_incoming.groupBy( "post_owner_id", "post_id")
      .agg(count("id").alias("incoming_comments_num"))
    com_incoming_num.show(10)

    com_incoming_num.write.parquet("task1_4_1.parquet")


  }
}
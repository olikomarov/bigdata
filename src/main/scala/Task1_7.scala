import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._

//count of open / closed (e.g. private) groups a user participates in

object Task1_7 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    println("count of open / closed (e.g. private) groups a user participates in")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()

    val groups = spark.read.parquet(s"D:\\bigdata_source\\groupsProfiles.parquet").select("id", "is_closed")
    val user_groups = spark.read.parquet(s"D:\\bigdata_source\\userGroupsSubs.parquet").select("group", "user")
    val user_groups2 = user_groups.withColumn("group_tmp", user_groups("group").cast(IntegerType))
      .drop("group")
      .withColumnRenamed("group_tmp", "id")
    val user_groups3 = user_groups2.withColumn("user_tmp", user_groups("user").cast(IntegerType))
      .drop("user")
      .withColumnRenamed("user_tmp", "user")
    val user_groups4 = user_groups3.withColumn("id", abs(user_groups3("id")))

    val merged = user_groups4.join(groups, Seq("id"))

    val groups_open = merged.filter(merged("is_closed") === 0)
    val groups_open_cnt = groups_open.groupBy("user").count()
    val groups_closed = merged.filter(merged("is_closed") === 1)
    val groups_closed_cnt = groups_closed.groupBy("user").count()

    val groups_fin1 = groups_open_cnt.join(groups_closed_cnt, Seq("user"))
    val groups_fin2 = Seq("from_id", "open_groups_count", "closed_groups_count")
    val groups_fin = groups_fin1.toDF(groups_fin2: _*)

    groups_fin.show(10)

    groups_fin.write.parquet("task1_7.parquet")
  }
}
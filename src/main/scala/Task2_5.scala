import com.vdurmont.emoji._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.collection.JavaConverters._

//find emoji (separately, count of: all, negative, positive, others)

object Task2_5 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    println("find emoji")

    val spark = SparkSession.builder()
      .appName("Task")
      .master("local[*]")
      .getOrCreate()

    val posts = spark.read.parquet("D:\\bigdata_source\\userWallPosts.parquet")
    val comments = spark.read.parquet("D:\\bigdata_source\\userWallComments.parquet")

    val negative = spark.read.csv("neg.csv")
    val negative_list = negative.rdd.map(r => r.getString(0)).collect.toList

    val positive = spark.read.csv("pos.csv")
    val positive_list = positive.rdd.map(r => r.getString(0)).collect.toList

    val neutral = spark.read.csv("neu.csv")
    val neutral_list = neutral.rdd.map(r => r.getString(0)).collect.toList

    val neg_emoji: (String) => (Int) = _text => {
      val inside = EmojiParser.extractEmojis(_text).asScala
      var nneg = 0
      for (_cur <- inside) {
        if (negative_list.contains(_cur)) {
          nneg += 1
        }
      }
      nneg
    }
    val neg_udf = udf(neg_emoji)

    val pos_emoji: (String) => (Int) = _text => {
      val inside = EmojiParser.extractEmojis(_text).asScala
      var npos = 0
      for (_cur <- inside) {
        if (positive_list.contains(_cur)) {
          npos += 1
        }
      }
      npos
    }
    val pos_udf = udf(pos_emoji)

    val neu_emoji: (String) => (Int) = _text => {
      val inside = EmojiParser.extractEmojis(_text).asScala
      var nneu = 0

      for (_cur <- inside) {
        if (neutral_list.contains(_cur)) {
          nneu += 1
        }
      }
      nneu
    }
    val neu_udf = udf(neu_emoji)

    val posts1 = posts.withColumn("Emoji_neg", neg_udf(col("text")))
    val posts2 = posts1.withColumn("Emoji_pos", pos_udf(col("text")))
    val posts3 = posts2.withColumn("Emoji_neu", neu_udf(col("text"))).select("from_id","Emoji_pos", "Emoji_neu", "Emoji_neg" )

    val comments1 = comments.withColumn("Emoji_neg", neg_udf(col("text")))
    val comments2 = comments1.withColumn("Emoji_pos", pos_udf(col("text")))
    val comments3 = comments2.withColumn("Emoji_neu", neu_udf(col("text"))).select("from_id","Emoji_pos", "Emoji_neu", "Emoji_neg" )

    val merged = posts3.union(comments3)

    val grouped = merged.groupBy("from_id").sum().select("from_id", "sum(Emoji_pos)", "sum(Emoji_neu)", "sum(Emoji_neg)")

    val cols = Seq("from_id", "Emoji_pos", "Emoji_neu", "Emoji_neg")
    val fin_res = grouped.toDF(cols: _*)
    fin_res.show(25)
    fin_res.write.parquet("task2_5.parquet")
  }
}
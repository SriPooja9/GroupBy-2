import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem2
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(
      ("Laptop","1000"),
      ("Phone","500"),
      ("T-Shirt","20"),
      ("Jeans","50"),
      ("Chair","150")
    ).toDF("product","price")

    data.groupBy($"product").agg(min($"price").alias("min_price")
      ,max($"price").alias("max_price"),avg($"price").alias("avg_price")).show()

  }

}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem17
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(
    ("1","2023-01-01"),
    ("1","2023-02-15"),
    ("2","2023-03-10"),
    ("2","2023-03-20"),
    ("3","2023-04-20"),
    ("3","2023-05-05")).toDF("customer_id","order_date")

    data.withColumn("month",month($"order_date")).groupBy($"customer_id")
      .agg(countDistinct($"month").alias("count")).filter($"count">1).show()

  }

}

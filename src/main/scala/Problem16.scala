import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem16
{

  def main(args:Array[String]): Unit = {

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(
    ("1","order_1","4","2"),
    ("1","order_2","5","1"),
    ("2","order_3","3","4"),
    ("2","order_4","4","3"),
    ("3","order_5","5","1")).toDF("product_id","order_id","rating","quantity")

    data.groupBy($"product_id").agg(avg($"rating").alias("avg_rating")).show()
    data.groupBy($"order_id").agg(sum($"quantity").alias("tot_quantity_sold")).show()

  }

}

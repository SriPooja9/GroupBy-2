import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem4
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val Data=List(
    ("Laptop","order_1","2"),
    ("Phone","order_2","1"),
    ("T-Shirt","order_1","3"),
    ("Jeans","order_3","4"),
    ("Chair","order_2","2")
    ).toDF("product","order_id","quantity")

    Data.groupBy($"order_id",$"product").agg(sum($"quantity").alias("quantity_sold"))
      .select($"order_id",$"product",$"quantity_sold").orderBy($"quantity_sold".desc)
    .limit(5).show()

  }

}

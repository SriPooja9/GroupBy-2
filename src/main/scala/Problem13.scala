import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem13
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

  val data=List(
    ("Laptop","Electronics","1000","2"),
    ("Phone","Electronics","500","1"),
    ("T-Shirt","Clothing","20","3"),
    ("Jeans","Clothing","50","4")).toDF("product","category","price","quantity")

    data.groupBy($"product").agg(avg($"price").alias("avg_price")).select($"product",$"avg_price")
      .show()

  }

}

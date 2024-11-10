import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem10
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(
      ("T-Shirt","Clothing","20"),
      ("Table","Furniture","150"),
      ("Jeans","Clothing","50"),
      ("Chair","Furniture","100")).toDF("product","category","price")

    data.filter(substring($"product",1,1)==="T").groupBy($"category").agg(avg($"price")
      .alias("avg_price")).show()

  }


}

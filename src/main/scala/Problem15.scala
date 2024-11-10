import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem15
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(
      ("Laptop","Electronics","2"),
      ("Phone","Electronics","1"),
      ("T-Shirt","Clothing","3"),
      ("Jeans","Clothing","4"),
      ("Chair","Furniture","2"),
      ("Sofa","Furniture","1")).toDF("product","category","quantity")


    data.groupBy($"category").agg(sum($"quantity").alias("quantity_sold")).orderBy($"quantity_sold".desc)
   . limit(2).show()

  }

}

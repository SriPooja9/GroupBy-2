import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.stddev

object Problem20 {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data = List(
      ("Laptop", "Electronics", "1000"),
      ("Phone", "Electronics", "500"),
      ("T-Shirt", "Clothing", "20"),
      ("Jeans", "Clothing", "50"),
      ("Chair", "Furniture", "150"),
      ("Sofa", "Furniture", "200")).toDF("product", "category", "price")

    data.groupBy($"category").agg(stddev($"price").alias("stand_dev_price")).show()

  }

}
import org.apache.spark.sql.SparkSession

object Problem1
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(
      ("Electronics","Laptop"),
      ("Electronics","Phone"),
      ("Clothing","T-Shirt"),
      ("Clothing","Jeans"),
      ("Furniture","Chair")).toDF("category","product")

    data.groupBy($"category").count().select($"category",$"count").show()


  }

}

import org.apache.spark.sql.SparkSession

object Problem7
{

  def main(args:Array[String]):Unit=
  {

  val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val Data=List(
      ("Laptop","2023-01-01"),
      ("Phone","2023-02-15"),
      ("T-Shirt","2023-03-10"),
      ("Jeans","2023-04-20")).toDF("product","order_date")

    Data.filter($"order_date"<="2023-02-01" or $"order_date">="2023-03-01").show()



  }

}

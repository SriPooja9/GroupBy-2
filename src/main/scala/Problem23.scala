import org.apache.spark.sql.SparkSession

object Problem23
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(
      ("1","2023-01-01","4"),
      ("1","2023-02-15","3"),
      ("2","2023-01-10","5"),
      ("2","2023-02-20","4"),
      ("3","2023-01-20","4"),
      ("3","2023-02-25","5")).toDF("product_id","order_date","rating")


  }

}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem9
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(
      ("2023-04-10","1","100"),
      ("2023-04-11","2","200"),
      ("2023-04-12","3","300"),
      ("2023-04-13","1","400"),
      ("2023-04-14","2","500")
    ).toDF("order_date","customer_id","amount")

    val df=data.withColumn("day",dayofweek($"order_date"))

    df.select(when($"day"===1 or $"day"===7,"weekend").otherwise("weekday").alias("new")
    ,$"day",$"amount").groupBy($"new").agg(avg($"amount").alias("avg_ordervalue")).show()

  }

}

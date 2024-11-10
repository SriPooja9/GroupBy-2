import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.dayofweek

object Problem24
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(
      ("order_1","2023-04-10","10"),
      ("order_2","2023-04-11","15"),
      ("order_3","2023-04-12","12"),
      ("order_4","2023-04-13","11"),
      ("order_5","2023-04-14","18")).toDF("order_id","order_date","hour")

    val df=data.withColumn("day",dayofweek($"order_date"))

    df.groupBy($"day",$"hour").count().select($"day",$"hour", $"count").show()
  }

}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object Problem18
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

  val data=List(
    ("USA","order_1","100"),
    ("USA","order_2","40"),
    ("UK","order_3","150"),
    ("France","order_4","250"),
    ("France","order_5","30")).toDF("country","order_id","amount")

    data.filter($"amount">50).groupBy($"country").agg(sum($"amount").alias("tot_salesamt")).show()

  }

}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem6
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._


    val Data=List(
      ("1","USA","order_1","100"),
      ("1","USA","order_2","200"),
      ("2","UK","order_3","150"),
      ("3","France","order_4","250"),
      ("3","France","order_5","300")).toDF("customer_id","country","order_id","amount")

    Data.groupBy($"country").agg(sum($"amount").alias("tot_amount")).show()

  }

}

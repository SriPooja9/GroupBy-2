import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Product11
{

  def main(args:Array[String]):Unit={


    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(
      ("1","order_1","100"),
      ("1","order_2","150"),
      ("2","order_3","250"),
      ("3","order_4","100"),
      ("3","order_5","120")).toDF("customer_id","order_id","amount")

    data.groupBy($"customer_id").agg(sum($"amount").alias("total")).filter($"total">200).show()

  }

}

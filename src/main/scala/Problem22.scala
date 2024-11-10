import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem22 {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data = List(
      ("1", "2022-01-01", "100"),
      ("1", "2023-02-15", "200"),
      ("2", "2022-03-10", "300")).toDF("customer_id", "order_date", "amount")

    data.groupBy($"customer_id",year($"order_date")).agg(sum($"amount").alias("CLTV")).show()

  }

}
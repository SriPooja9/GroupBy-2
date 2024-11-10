import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem3 {

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val Data=List(
      ("2023-01-01","New York","100"),
      ("2023-02-15","London","200"),
      ("2023-03-10","Paris","300"),
      ("2023-04-20","Berlin","400"),
      ("2023-05-05","Tokyo","500")
    ).toDF("order_date","city","amount")

Data.withColumn("year",year($"order_date"))
  .withColumn("month",month($"order_date"))
  .groupBy($"year",$"month").agg(sum($"amount").alias("amount"))
  .show()

  }

}

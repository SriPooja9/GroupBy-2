import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem12 {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(
      ("order_1","150"),
      ("order_2","80"),
      ("order_3","220"),
      ("order_4","50")).toDF("order_id","amount")

    val df=data.groupBy($"order_id").agg(sum($"amount").alias("amount"))

    df.withColumn("order_status",when($"amount">100,"High").otherwise("Low")).show()



  }

}
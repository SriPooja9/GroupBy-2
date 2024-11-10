import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem25 {

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(("USA","order_1","100","Shipped"),
    ("USA","order_2","40","Cancelled"),
    ("UK","order_3","150","Completed"),
    ("France","order_4","250","Pending"),
    ("France","order_5","30","Shipped")).toDF("country","order_id","amount","status")

    val df=data.filter($"status"=!="Cancelled")

df.groupBy($"country").agg(avg($"amount").alias("avg_amount")).show()

  }

}

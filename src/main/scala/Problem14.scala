import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem14
{

  def main(args:Array[String]):Unit={

    val conf=new SparkConf()
    conf.set("spark.appname","Sri").set("spark.master","local[*]")

    val spark=SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val data=List(
      ("2023-01-01","1","100"),
      ("2023-02-15","2","200"),
      ("2023-03-10","3","300"),
      ("2023-04-20","1","400"),
      ("2023-05-05","2","500")).toDF("order_date","customer_id","amount")

    val df=data.withColumn("year",year($"order_date")).withColumn("month",month($"order_date"))

      val year_wise=df.groupBy($"year").agg(count($"year").alias("cnt"),sum($"amount").alias("tot_amount"))
      val month_wise=df.groupBy($"month").agg(count($"month").alias("cnt"),sum($"amount").alias("tot_amount"))
      val month_year_wise=df.groupBy($"year",$"month").agg(count($"order_date").alias("cnt"),sum($"amount").alias("tot_amount"))


    //year_wise.show()
    //month_wise.show()
    //df.show()
    month_year_wise.show()

  }

}

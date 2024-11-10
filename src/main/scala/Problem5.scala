import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem5
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(
      ("1","1","4"),
      ("1","2","5"),
      ("2","1","3"),
      ("2","3","4"),
      ("3","2","5")).toDF("user_id","product_id","rating")

    data.groupBy($"user_id").agg(avg($"rating").alias("avg_rating")).show()

  }

}

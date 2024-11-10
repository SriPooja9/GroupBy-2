import org.apache.spark.sql.SparkSession

object Problem8
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(
      ("1","New York","order_1"),
      ("1","New York","order")
    ).toDF("customer_id","city","order_id")

    data.groupBy($"customer_id",$"city").count().select($"customer_id",$"city",$"count").show()

  }

}

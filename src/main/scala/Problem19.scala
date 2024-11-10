import org.apache.spark.sql.SparkSession

object Problem19
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(
      ("order_1","1","2"),
      ("order_2","1","3"),
      ("order_3","2","4"),
      ("order_4","3","1")).toDF("order_id","product_id1","product_id2")



  }

}

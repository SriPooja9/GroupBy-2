import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Problem21
{

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val data=List(
    ("1","New York"),
    ("1","New York"),
    ("2","London"),
    ("2","Paris"),
    ("3","Paris"),
    ("3","Paris")).toDF("customer_id","city")

    val df=data.groupBy($"customer_id",$"city").count().select($"customer_id",$"city",$"count")

    val wind=Window.partitionBy($"customer_id").orderBy($"count")

    val df1=df.withColumn("rnk",row_number().over(wind)).filter($"rnk"===1)

    df.show()
    df1.show()

  }


}

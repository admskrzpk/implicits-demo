
object ImplcitsDemo extends App {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  val dept = Seq(
    ("50000.0#0#0#", "#"),
    ("0@1000.0@", "@"),
    ("1$", "$"),
    ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")

  val result2 = dept.withColumn("split_values", expr("""split(VALUES, concat("\\", Delimiter))""")).show
}

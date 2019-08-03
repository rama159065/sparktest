package spark.com.test.spark

import org.apache.spark.sql.SparkSession

object ReadCSV extends App {

  val session = SparkSession.builder().master("local").appName("Read CSV").getOrCreate();
  val df = session.read.option("header", "true")csv("D:\\Upgrad\\Statistical\\EDA\\grades.csv")

  val df1 = session.read.option("header", "true")csv("D:\\Upgrad\\Statistical\\EDA\\grades.csv")
  df.show(10)

  val jdf = df.join(df1, Seq("submission", "inner"))
  import session.implicits._

  val ds = session.read.text("src/main/resources/data.txt").as[String]

  val sqlContext = session.sqlContext

  case class University(name: String, numStudents: Long, yearFounded: Long)







}

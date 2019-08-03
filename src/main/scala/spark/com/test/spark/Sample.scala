package spark.com.test.spark

import org.apache.spark.sql.SparkSession

object Sample extends App {

  val spark=SparkSession.builder().master("local").appName("Spark Test").getOrCreate()

  import spark.implicits._
  // Encoders are created for case classes
  case class Employee(name: String, age: Long)
  val caseClassDS = Seq(Employee("Amy", 32)).toDS
  caseClassDS.show()
  // convert DataFrame to strongly typed Dataset
  case class Movie(actor_name:String, movie_title:String, produced_year:Long)
  val movies = Seq(("Damon Matt", "The Bourne Ultimatum", 2007L),
    ("Damon Matt", "Good Will Hunting", 1997L))
  val moviesDF = movies.toDF.as[Movie]
  moviesDF.show(10)

}

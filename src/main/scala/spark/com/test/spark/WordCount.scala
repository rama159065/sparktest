package spark.com.test.spark

import org.apache.spark.sql.SparkSession

object WordCount extends App {

  val spark=SparkSession.builder().master("local").appName("WC Test").getOrCreate()
  var sc = spark.sparkContext

  import spark.implicits._

  val wordsDataset = sc.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDS()
  val groupedDataset = wordsDataset.flatMap(_.toLowerCase.split(" "))
    .filter(_ != "")
    .groupBy("value")
  val countsDataset = groupedDataset.count()
  countsDataset.show()

}

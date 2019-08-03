package spark

import org.apache.spark.sql.{Dataset, Row, SparkSession}


object CampaignMetricsScala extends App {

  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("CampaignMetrics")
    .config("spark.driver.host", "localhost")
    .getOrCreate()


  /**
    * +-----------+-------------+---------------+--------------+-----------------------------+-----------------------------+
    * |campaign_id|campaign_name|channel        |deals         |end_date                     |start_date                   |
    * +-----------+-------------+---------------+--------------+-----------------------------+-----------------------------+
    * |campaign1  |TestCampaign |[mobile, email]|[deal1, deal2]|2019-01-01T12:00:00.000+05:30|2018-12-07T14:01:10.000+05:30|
    * +-----------+-------------+---------------+--------------+-----------------------------+-----------------------------+
    */

  /**
    * +-------+--------------+----------+-----------+
    * |deal_id|disount_amount|product_id|active_flag|
    * +-------+--------------+----------+-----------+
    * |deal1  |1.0           |product1  |Y          |
    * |deal2  |1.2           |product2  |Y          |
    * |deal1  |1.5           |product3  |N          |
    * |deal1  |1.2           |product4  |Y          |
    * |deal1  |1.2           |product2  |Y          |
    * +-------+--------------+----------+-----------+
    */


  /**
    * +----------+-----------+
    * |product_id|actualPrice|
    * +----------+-----------+
    * |product1  |10.0       |
    * |product2  |5.0        |
    * |product3  |20.0       |
    * |product4  |8.0        |
    * +----------+-----------+
    */


  /**
    * +--------------+----------+--------+----------------+
    * |transaction_id|product_id|deal_id |transaction_date|
    * +--------------+----------+--------+----------------+
    * |1             |deal1     |product1|2018-12-09      |
    * |2             |deal2     |product2|2018-12-17      |
    * |3             |deal1     |product4|2018-12-22      |
    * |4             |deal1     |product2|2018-12-08      |
    * +--------------+----------+--------+----------------+
    */

  /**
    * Feel free to change the method definition or pass extra arguments if required
    */

  /**
    * Question 1 : Create Datasets for deals, product,transaction from csv and for campaign from json
    * @return
    */
  def dealsDF(): Dataset[Row] = {
    spark.read.json("")
  }

  def productDF(): Dataset[Row] = ???

  def transactionDF(): Dataset[Row] = ???

  def JsonDF(): Dataset[Row] = ???

  /**
    * Question 2 : Read the Campaign.json file located in the resources folder and explode the dataset.
    * explodedCampaignDf take n number of parameters as input. Please choose on your own
    * @return
    */
  def explodedCampaignDF(): Dataset[Row] = ???

  /**
    * Question 3: Create a Denormalized output with below-given columns from Campaign, Deal and Product Datasets.
    * campaign_id, campaign_name, start_date, end_date, channel, deal_id, discount_amount, active_flag, product_id, actual_price
    * campaignDealsProductDf take n number of parameters as input. Please choose on your own
    *
    */
  def campaignDealsProductDf(): Dataset[Row] = ???


  /**
    * Question 4 : Compute the total actual amount ( actual_price) for active deals at campaign channel and transaction date level
    * activeDealsActualPriceChannel take n number of parameters as input. Please choose on your own
    *
    */
  def activeDealsActualPriceChannel(): Dataset[Row] = ???

  /**
    * Question 5: Compute the total amount (actual_price - discount_amount), total transactions for active deals at each deal and transaction date level
    * totalTransactionTotalAmountActive take n number of parameters as input. Please choose on your own
    *
    */

  def totalTransactionTotalAmountActive():Dataset[Row] = ???
  /**
    * Question 6: For each deal, identify the latest transaction using Transaction Dataset
    * dealwiseLatestTxnData take n number of parameters as input. Please choose on your own
    *
    */
  def dealwiseLatestTxnData():Dataset[Row] = ???


}

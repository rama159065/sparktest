package com.test.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import scala.collection.JavaConversions;

public class CampaignMetricsJava {

    public static SparkSession spark = null;

    static String path = "D:\\sparktest\\src\\main\\resources\\";

    public CampaignMetricsJava() {
        spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("CampaignMetricsJava")
                .config("spark.driver.host", "localhost")
                .getOrCreate();
    }

    /**
     * Question1: reate Datasets for deals, product,transaction from csv and for campaign from json
     * @return
     */

    private static Dataset<Row> jsonDf() {
        Dataset<Row> campains = spark.read().json(path+"campaign.json");


        return campains;
    }

    private static Dataset<Row> dealsDf() {
        Dataset<Row> deals = spark.read().option("header", "true").csv(path+"deals.csv");

        return deals;
    }

    private static Dataset<Row> productDf() {
        Dataset<Row> products = spark.read().option("header", "true").csv(path+"product.csv");
        return products;
    }

    private static Dataset<Row> transactionDf() {
        Dataset<Row> transactions = spark.read().option("header", "true").csv(path+"transaction.csv");
        return transactions;
    }


    /**
     * Question 2 : Read the Campaign.json file located in the resources folder and explode the dataset.
     * explodedJsonDf take n number of parameters as input. Please choose on your own
     * @return
     */
    public Dataset<Row> explodedCampaignDf()  {

        Dataset<Row> campains = jsonDf();
        campains = campains.select(col("*"), explode(col("channel")).alias("channel_exp"));
        campains = campains.drop(col("channel")).withColumnRenamed("channel_exp", "channel");
        campains = campains.select(col("*"), explode(col("deals")).alias("deals_exp"));
        campains = campains.drop(col("deals")).withColumnRenamed("deals_exp", "deals");
        campains.show();
        return campains;
    }

    /**
     * Question 3: Create a Denormalized output with below-given columns from Campaign, Deal and Product Datasets.
     * campaign_id, campaign_name, start_date, end_date, channel, deal_id, discount_amount, active_flag, product_id, actual_price
     * campaignDealsProductDf take n number of parameters as input. Please choose on your own
     *
     */
    public Dataset<Row> campaignDealsProductDf() {

        Dataset<Row> campaigns = explodedCampaignDf();
        Dataset<Row> deals = dealsDf();
        Dataset<Row> products = productDf();
        Dataset<Row> join = deals.join(campaigns, campaigns.col("deals").equalTo(deals.col("deal_id")))
                .join(products, deals.col("product_id").equalTo(products.col("product_id")));

        join.show();

        return join;
    }


    /**
     * Question 4 : Compute the total actual amount ( actual_price) for active deals at campaign channel and transaction date level
     * activeDealsActualPriceChannel take n number of parameters as input. Please choose on your own
     *
     */

    public Dataset<Row> activeDealsActualPriceChannel() {

        Dataset<Row> campDealProd = campaignDealsProductDf();
        campDealProd = campDealProd.filter(campDealProd.col("active_flag").equalTo("Y"));
        Dataset<Row> transactions = transactionDf();
        Dataset<Row> join = campDealProd.join(transactions, campDealProd.col("deal_id").equalTo(transactions.col("deal_id")));
        Dataset<Row> actuals = join.groupBy("channel", "transaction_date").agg(sum("actual_price"));
        actuals.show();

        return actuals;
    }

    /**
     * Question 5: Compute the total amount (actual_price - discount_amount), total transactions for active deals at each deal and transaction date level
     * totalTransactionTotalAmountActive take n number of parameters as input. Please choose on your own
     *
     */

    public Dataset<Row> totalTransactionTotalAmountActive() {

        Dataset<Row> campDealProd = campaignDealsProductDf();
        campDealProd = campDealProd.filter(campDealProd.col("active_flag").equalTo("Y"));
        Dataset<Row> transactions = transactionDf();
        Dataset<Row> join = campDealProd.join(transactions, campDealProd.col("deal_id").equalTo(transactions.col("deal_id")));
        join = join.withColumn("actual_with_discount", lit(col("actual_price").minus(col("discount_amount"))));
        Dataset<Row> actuals_with_discount = join.groupBy("channel", "transaction_date").agg(sum("actual_with_discount"));
        actuals_with_discount.show();
        return actuals_with_discount;
    }


    /**
     * Question 6: For each deal, identify the latest transaction using Transaction Dataset
     * dealwiseLatestTxnData take n number of parameters as input. Please choose on your own
     *
     */

    public Dataset<Row> dealwiseLatestTxnData() {
        Dataset<Row> deals = dealsDf();
        Dataset<Row> transactions = transactionDf();
        Dataset<Row> join = deals.join(transactions, "deal_id");
        Dataset<Row> dealsLatestTransaction = join.groupBy("deal_id").agg(max("transaction_date"));
        dealsLatestTransaction.show();
        return dealsLatestTransaction;
    }



    public static void main(String[] args) {
        CampaignMetricsJava campaignMetricsJava = new CampaignMetricsJava();
        campaignMetricsJava.explodedCampaignDf();
        campaignMetricsJava.campaignDealsProductDf();
        campaignMetricsJava.activeDealsActualPriceChannel();
        campaignMetricsJava.totalTransactionTotalAmountActive();
        campaignMetricsJava.dealwiseLatestTxnData();

    }
}

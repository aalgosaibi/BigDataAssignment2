package com.spark.assignment1

import com.spark.assignment2.{Assignment2, Business, Review, User}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterEach

import scala.concurrent.duration._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class Assignment2Test extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  /**
   * Set this value to 'true' to halt after execution so you can view the Spark UI at localhost:4040.
   * NOTE: If you use this, you must terminate your test manually.
   * OTHER NOTE: You should only use this if you run a test individually.
   */
  val BLOCK_ON_COMPLETION = false;

  // Paths to dour data.
  val business_DATA_CSV_PATH = "data/yelp_business.csv"
  val review_DATA_CSV_PATH = "data/yelp_review_reduced.csv"
  val USER_DATA_CSV_PATH = "data/yelp_user_reduced.csv"
  /**
   * Create a SparkSession that runs locally on our laptop.
   */
  val spark =
    SparkSession
      .builder()
      .appName("Assignment 1")
      .master("local[*]") // Spark runs in 'local' mode using all cores
      .getOrCreate()

  /**
   * Encoders to assist converting a csv records into Case Classes.
   * They are 'implicit', meaning they will be picked up by implicit arguments,
   * which are hidden from view but automatically applied.
   */

  implicit val businessEncoder: Encoder[Business] = Encoders.product[Business]
  implicit val reviewEncoder: Encoder[Review] = Encoders.product[Review]
  implicit val userEncoder: Encoder[User] = Encoders.product[User]
  /**
   * Let Spark infer the data types. Tell Spark this CSV has a header line.
   */
  val csvReadOptions =
    Map("inferSchema" -> true.toString, "header" -> true.toString)

  /**
   * Create business Spark collections
   */
  def businessDataDS: Dataset[Business] = spark.read.options(csvReadOptions).csv(business_DATA_CSV_PATH).as[Business]
  def businessDataDF: DataFrame = businessDataDS.toDF().na.drop()
  businessDataDF.write.mode(SaveMode.Overwrite).parquet("data/businessData.parquet")
  def businessData_parquetDF = spark.read.parquet("data/businessData.parquet")
  /**
   * Create business Spark collections
   */
  def reviewDataDS: Dataset[Review] = spark.read.options(csvReadOptions).csv(review_DATA_CSV_PATH).as[Review]
  def reviewDataDF: DataFrame = reviewDataDS.toDF().na.drop()
  reviewDataDF.write.mode(SaveMode.Overwrite).parquet("data/review_data_reduced.parquet")
  def reviewDataDF_parquetDF = spark.read.parquet("data/review_data_reduced.parquet")

  /**
   * Create user Spark collections
   */
  def userDataDS: Dataset[User] = spark.read.options(csvReadOptions).csv(USER_DATA_CSV_PATH).as[User]
  def userDataDF: DataFrame = userDataDS.toDF()
  userDataDF.write.mode(SaveMode.Overwrite).parquet("data/user_data_reduced.parquet")
  def userDataDF_parquetDF: DataFrame = spark.read.parquet("data/user_data_reduced.parquet")
  /**
   * Keep the Spark Context running so the Spark UI can be viewed after the test has completed.
   * This is enabled by setting `BLOCK_ON_COMPLETION = true` above.
   */
  override def afterEach: Unit = {
    if (BLOCK_ON_COMPLETION) {
      // open SparkUI at http://localhost:4040
      Thread.sleep(5.minutes.toMillis)
    }
  }

  /*
   * DATAFRAMES
   */

  /**
   * Count the businesses in Las Vegas
   */
  test("Count the businesses in Las Vegas") {
    Assignment2.dfProblem1(businessData_parquetDF).count() must equal (12900)
  }

  /**
   * Count the number of Subway in Las Vegas
   */
  test("Count the number of Subway in Las Vegas") {
    Assignment2.dfProblem2(businessData_parquetDF).count() must equal (58)
  }

  /**
   * Count the business in Las Vegas that has more than 2 stars
   */
  test("Count the business in Las Vages that has 2 stars") {
    Assignment2.dfProblem3(businessData_parquetDF).count() must equal (37)
  }

  /**
   * Count the business in Las Vegas that has more than 100 review count
   */
  test("Count the business in Las Vages that has more than 100 review count") {
    Assignment2.dfProblem4(businessData_parquetDF).count() must equal (1616)
  }

  /**
   * Count all the number of users who gave more than 4 stars to businesses in Las Vegas
   */
  test("Count all the users who gave more than 4 stars to businesses in Las Vegas") {
    Assignment2.dfProblem5(reviewDataDF_parquetDF,userDataDF_parquetDF,businessData_parquetDF).count() must equal (2710)
  }

  /**
   * My Hypothesis is to compare average stars between To start with cleansedBusinessDF, then perform a narrow transformation, and finally to an array. vs las Vegas based users' review. It shows
   * that the customer service is good
   */
  test("My Hypothesis is to compare average stars between To start with cleansedBusinessDF, then perform a narrow transformation, and finally to an array. vs las Vegas based users' review. It shows that the customer service is bad") {
    Assignment2.dfProblem6(reviewDataDF_parquetDF,userDataDF_parquetDF,businessData_parquetDF)
  }

  /**
   * Average number of stars and review count of restaurants based users' review
   */
  test("Average number of stars and review count of restaurants based users' review") {
    Assignment2.dfProblem7(reviewDataDF_parquetDF,userDataDF_parquetDF)
  }

}

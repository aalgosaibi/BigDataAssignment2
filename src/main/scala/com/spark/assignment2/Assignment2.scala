package com.spark.assignment2

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SaveMode, SparkSession}

object Assignment2 {

  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("M/d/yyyy H:mm")

  /**
    * Helper function to print out the contents of an RDD
    * @param label Label for easy searching in logs
    * @param theRdd The RDD to be printed
    * @param limit Number of elements to print
    */
  private def printRdd[_](label: String, theRdd: RDD[_], limit: Integer = 20) = {
    val limitedSizeRdd = theRdd.take(limit)
    println(s"""$label ${limitedSizeRdd.toList.mkString(",")}""")
  }

  def removeQoute(business: DataFrame): DataFrame ={
    regexp_replace(business("name"), "\"\"\"", "")
    val newDF = business.withColumn("removeDoubleQuotesName",regexp_replace(business("name"), "\"\"\"", ""))
    return newDF
  }

  def dfProblem1(business: DataFrame): DataFrame = {
    business.filter("city == 'Las Vegas'").select("city")
  }

  def dfProblem2(business: DataFrame): DataFrame = {
    val cleanBusinessDF = removeQoute(business)
    cleanBusinessDF.filter("removeDoubleQuotesName == 'Subway' and city=='Las Vegas'").select("removeDoubleQuotesName")
  }

  def dfProblem3(business: DataFrame): DataFrame = {
    val businessDF = removeQoute(business)
    businessDF.filter("removeDoubleQuotesName == 'Subway' and stars > '2.0' and city=='Las Vegas'").select("removeDoubleQuotesName")
  }

  def dfProblem4(business: DataFrame): DataFrame = {
    val businessDF = removeQoute(business)
    businessDF.filter("review_count > 100 and city=='Las Vegas'").select("city")
  }

  def dfProblem5(review: DataFrame, user:DataFrame, business: DataFrame): DataFrame = {
    val lasVegasBusiness = business.filter("city=='Las Vegas'")
    val startReview = review.filter("stars > 4")
    lasVegasBusiness.join(startReview,lasVegasBusiness("business_id") === startReview("business_id"),"inner")//.join(user, startReview("user_id") === user("user_id"),"inner").select(user("name"))
  }

  def dfProblem6(review: DataFrame, user:DataFrame, business: DataFrame): Unit = {
    val cleanBusinessDF = removeQoute(business)
    val ClevelandSubwayBusiness = cleanBusinessDF.filter("removeDoubleQuotesName == 'Subway' and city=='Cleveland'")
    val lasVegasSubwayBusiness = cleanBusinessDF.filter("removeDoubleQuotesName == 'Subway' and city=='Las Vegas'")

    val lasVegasUserReviewStar = lasVegasSubwayBusiness.join(review,lasVegasSubwayBusiness("business_id") === review("business_id"),"inner").join(user, review("user_id") === user("user_id"),"inner")
    val TorontoUserReviewStar = ClevelandSubwayBusiness.join(review,ClevelandSubwayBusiness("business_id") === review("business_id"),"inner").join(user, review("user_id") === user("user_id"),"inner")

    lasVegasUserReviewStar.agg(avg(lasVegasSubwayBusiness("stars")))
    TorontoUserReviewStar.agg(avg(ClevelandSubwayBusiness("stars")))
  }

  def dfProblem7(review: DataFrame, user:DataFrame): Unit = {
    val nonEliteUser= user.filter("elite == 'None'")
    val usefulReview = review.filter("useful == 1")
    val avgStars = nonEliteUser.join(usefulReview, nonEliteUser("user_id") === usefulReview("user_id"),"inner")
    avgStars.agg(avg(usefulReview("stars")), avg(nonEliteUser("review_count"))).show()
  }

  // Helper function to parse the timestamp format used in the trip dataset.
  private def parseTimestamp(timestamp: String) = LocalDateTime.from(timestampFormat.parse(timestamp))
}

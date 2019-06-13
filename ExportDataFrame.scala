package com.conns

import java.sql.{ResultSet, ResultSetMetaData}

import com.conns.Utils.makeConnection
import org.apache.spark.sql.{DataFrame, SparkSession}
object ExportDataFrame {

  def readWrittenSalesData(sparkSession: SparkSession): DataFrame =
  {
    val driver = "org.apache.hive.jdbc.HiveDriver"
    val url = "jdbc:hive2://172.16.100.52:10000"
    val username = "awarkhade"
    val password = "Conns123"

    var finalSeq = Seq[Sales]()

    try {
      val connection = makeConnection(driver,url,username,password)
      val statement = connection.createStatement()
      val resultSet: ResultSet = statement.executeQuery("select product_class ,product_category ,order_date, location_Code, sum(written_sales)" +
        "from written_sales_pz.written_sales_product where order_date = '2019-01-21' " +
        "group by product_class ,product_category ,order_date, location_code " +
        "order by product_class ,product_category , order_date, location_code "
      )
      val metadata: ResultSetMetaData = resultSet.getMetaData
      while (resultSet.next()) {
        val product_class = resultSet.getString("product_class") //the names has to be matching
        val product_category = resultSet.getString("product_category")
        val order_date = resultSet.getDate("order_date")
        val location = resultSet.getString("location_code")
        val sales = resultSet.getDouble("_c4")
        val fetchedSeq = Seq(Sales(product_class,product_category,location,order_date,sales))
        finalSeq = finalSeq ++ fetchedSeq
      }
      connection.close()
    }
    catch {
      case e :Exception => e.printStackTrace()
    }

    import sparkSession.implicits._
    finalSeq.toDF

  }
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DataFrame Exporter")
      .config("spark.master", "local")
      .getOrCreate()

    val cleanedDF = readWrittenSalesData(spark)
    cleanedDF.show(cleanedDF.count().toInt)

    cleanedDF.coalesce(1).write
      .format("org.zuinnote.spark.office.excel")
      .option("spark.write.useHeader","true")
      .option("write.locale.bcp47", "us")
      .save("E:\\ConnsKPI scripts\\WrittenSales\\WrittenSalesValidation\\ExportedData.xlsx")
  }

}

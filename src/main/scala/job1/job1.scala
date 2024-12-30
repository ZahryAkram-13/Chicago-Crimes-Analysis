package job1

import org.apache.spark.sql.SparkSession

object job1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Job1")
      .getOrCreate()

    // Your Spark logic here
    println("Running Job1")

    spark.stop()
  }
}

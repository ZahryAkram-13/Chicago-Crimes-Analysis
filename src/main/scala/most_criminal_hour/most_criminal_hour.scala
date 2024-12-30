package most_criminal_hour

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import commun.Util


/**
 * This object analyzes the crime data to identify the hour of the day with the highest crime rates.
 * It processes the time and AM/PM columns to convert them into a 24-hour format and then groups the data
 * by the hour. It aggregates the count of crimes for each hour and displays the top 10 hours with the 
 * highest crime counts.
 * 
 * The results are saved to a specified location for further analysis or reporting.
 */
object most_criminal_hour{

    def main(args: Array[String]): Unit = {
        val spark = Util.get_session("most_criminal_hour")

        val data: DataFrame = Util.get_transformed_data(spark)

        val time_column: String = "Time"
        val midday_column: String = "AM/PM"

        val data_with_hour = data.withColumn(
            "hour_exact",
            when(
                col(midday_column) === "AM",
                when(
                    split(col(time_column), ":").getItem(0).cast("int") === 12,
                    lit(0)
                )
                .otherwise(split(col(time_column), ":").getItem(0).cast("int"))
            )
            .otherwise(
                when(
                    split(col(time_column), ":").getItem(0).cast("int") === 12,
                    lit(12)
                )
                .otherwise(split(col(time_column), ":").getItem(0).cast("int") +12)
            )
        )

        val crime_by_hour = data_with_hour
        .groupBy("hour_exact")
        .agg(count("*").as("crime_count"))
        .orderBy(desc("crime_count"))

        crime_by_hour.limit(10).show()

        Util.save_data(crime_by_hour, "most_criminal_hour")


        spark.stop()
    }
}
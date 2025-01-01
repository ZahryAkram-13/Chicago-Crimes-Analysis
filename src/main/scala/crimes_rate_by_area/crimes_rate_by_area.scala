package crimes_rate_by_area

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import commun.Util



/**
 * This Spark application calculates crime and arrest statistics for different community areas.
 * It computes the total number of crimes, total arrests, and the arrest rate (percentage of arrests per total crimes).
 * The application then displays the top 5 areas with the highest arrest rates and crime rates, and saves the results with the arrest rates to an output folder.
 */
object crimes_rate_by_area{

    def main(args: Array[String]): Unit = {
        val spark = Util.get_session("crimes_rate_by_area")

        val data = Util.get_transformed_data(spark)

        val area_arrests_columns = Seq("Community_Area", "Arrest")
        val area_stat = data.groupBy("Community_Area")
        .agg(
            count("*").as("total_crimes"),
            sum(when(col("Arrest") === "true", 1).otherwise(0)).as("total_arrests")
        )

        val area_rate = area_stat.withColumn(
            "arrest_rate",
            (col("total_arrests") / col("total_crimes"))*100
        )

        area_rate.limit(20).show()

        println("5 Area with the highest arrest rate:")
        val highest5_arrest_area_rate = area_rate.orderBy(desc("arrest_rate")).limit(5).show()
        
        val highest5_crimes_area_rate = area_stat.orderBy(desc("total_crimes")).limit(5).show()


        val output_folder = Util.get_output_folder("crimes_rate_by_area");
        Util.save_data(area_rate, output_folder)


        spark.stop()
    }   
}
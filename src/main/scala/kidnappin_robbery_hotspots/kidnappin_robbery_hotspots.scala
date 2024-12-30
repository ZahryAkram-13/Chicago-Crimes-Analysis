package kidnappin_robbery_hotspots

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import commun.Util

/**
 * This application identifies and analyzes hotspots for Kidnapping and Robbery crimes 
 * based on their location and type, displaying the top 20 hotspots.
 */
object kidnappin_robbery_hotspots{
    
    def main(args: Array[String]): Unit = {

        val spark = Util.get_session("kidnappin_robbery_hotspots")

        val data = Util.get_transformed_data(spark)

        val Primary_type_column = "Primary Type"
        val location_column = "Location"
        val location_description_column = "Location Description"

        val filtred_data = data
        .filter(col(Primary_type_column).isin("KIDNAPPING", "ROBBERY"))
        .filter(!col(location_column).isNaN || !col(location_column).isNotNull)

        val grouped = filtred_data
        .groupBy(Primary_type_column, location_column, location_description_column)
        .count()
        .orderBy(desc("count"))

        Util.print_value("value", Primary_type_column)
        grouped.limit(30).show(truncate = false)

        Util.save_data(grouped, "kidnappin_robbery_hotspots")

        spark.stop()

    }
}
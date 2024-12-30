package commun_type_by_district

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import commun.Util

/**
 * This application analyzes the distribution of crime types by district, 
 * counting the number of crimes for each combination of 'District' and 'Primary Type'. 
 * The results are displayed and saved to a specified output folder.
 */
object commun_type_by_district {
    def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("commun_type_by_district")
      .getOrCreate()

    val data_file = Util.data_file
    val output_path = Util.output_path

    val data = spark.read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(data_file) 
    
    val crime_by_district = data.groupBy("District", "Primary Type")
                                .agg(count("*").alias("crime_count"))
                                .orderBy("District", "crime_count")               

    crime_by_district.show(truncate = false)

    val output_folder = s"${output_path}/commun_type_by_district"
    crime_by_district.write.option("header", "true").mode("overwrite").csv(output_folder) 


    spark.stop()
  }
}

package commun

import org.apache.spark.sql.{DataFrame, SparkSession}

object Util {
  // Define paths here
  val data_path: String = "gs://dataproc-staging-us-east1-124100932800-zukothwe/data" 
  val data_file: String = s"${data_path}/chicago_crime.csv" 
  val transformed_data_path: String = "gs://dataproc-staging-us-east1-124100932800-zukothwe/data/transformed_data"  

  val output_path: String = "gs://dataproc-staging-us-east1-124100932800-zukothwe/outputs"  



  def get_session(name : String): SparkSession = {
    SparkSession.builder().appName(name).getOrCreate()
  }

  def get_data(spark : SparkSession): DataFrame = {
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(data_file)
  }

  def get_transformed_data(spark: SparkSession): DataFrame = {
  try {
    spark.read
      .format("csv")
      .option("header", "true")
      .load(transformed_data_path)
  } catch {
    case e: Exception =>
      println(s"Error loading data from $transformed_data_path: ${e.getMessage}")
      throw e 
  }
}

  def save_data(df: DataFrame, path: String): Unit = {
    df.write.option("header", "true").mode("overwrite").csv(path)
  }

  def get_output_folder(folder_name: String): String = {
    s"${output_path}/${folder_name}"
  }

  def print_value(name: String, value: Any): Unit = {
    println(s"---------------${name}---------------------------")
    println(value)
    println("---------------------------------------------------")
  }
}

/** 
columns :
Index(['ID', 'Case Number', 'Date', 'Block', 'IUCR', 'Primary Type',
       'Description', 'Location Description', 'Arrest', 'Domestic', 'Beat',
       'District', 'Ward', 'Community Area', 'FBI Code', 'X Coordinate',
       'Y Coordinate', 'Year', 'Updated On', 'Latitude', 'Longitude',
       'Location'],
      dtype='object')
**/
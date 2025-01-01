file://<WORKSPACE>/transform_columns/transform_columns.scala
### java.lang.IndexOutOfBoundsException: -1

occurred in the presentation compiler.

presentation compiler configuration:


action parameters:
offset: 3453
uri: file://<WORKSPACE>/transform_columns/transform_columns.scala
text:
```scala
package transform_columns

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import commun.Util


/**
 * This application transforms a dataset by dropping unnecessary columns, 
 * splitting the 'Date' column into separate 'Date', 'Time', and 'AM_PM' columns, 
 * and filling missing values in the 'Location Description' and 'Community Area' columns 
 * with their most frequent values.
 */
object transform_columns{

    def most_frequent_value(df: DataFrame, col_name: String): String = {
        val most_frequent = df
        .groupBy(col_name)
        .count()
        .orderBy(desc("count"))
        .first()
        most_frequent.getString(0)
    }

    def clean_invalid_char(df: DataFrame, col_name: String): DataFrame = {
        val clean = udf((value: String) => 
            if(value != null) value.replaceAll("[^a-zA-Z0-9_]", "_") 
            else null
        )

        df.withColumn(
            col_name,
            clean(col(col_name))
        )
    }

    def main(args: Array[String]): Unit = {

        val spark = Util.get_session("transform_colomns")

        val data = Util.get_data(spark)

        val dropped_columns = Seq("Updated On", "ID", "Case Number", "FBI Code", "Ward", "Block")
        var new_data = dropped_columns.foldLeft(data){
            (tmp_df, col) => tmp_df.drop(col)
            }

        println(s"shape:${new_data.columns.length}")

        /**
        change th date and splite to date and time

        val sqlQuery = """
        SELECT 
        SPLIT(Date, ' ')[0] AS Date, 
        SPLIT(Date, ' ')[1] AS Time,
        SPLIT(Date, ' ')[2] AS AM_PM,
        SPLIT(SPLIT(Date, ' ')[1], ':')[0] AS Hour,
        SPLIT(SPLIT(Date, ' ')[1], ':')[1] AS Minute,
        SPLIT(SPLIT(Date, ' ')[1], ':')[2] AS Second
        FROM data_view
        """
        **/
        new_data = new_data
        .withColumn("Time", split(col("Date"), " ").getItem(1))
        .withColumn("AM_PM", split(col("Date"), " ").getItem(2))
        .withColumn("Date", split(col("Date"), " ").getItem(0))


        new_data.select("Date", "Time", "AM_PM").limit(20).show()

        /**
        fill missing values of Location Description with most frequent location
        **/
        val location_description = "Location Description"
        val frequent_location = most_frequent_value(new_data, location_description)

        new_data = new_data.withColumn(
            location_description,
            when(isnan(col(location_description)), lit(frequent_location)).otherwise(col(location_description))
        )

        new_data.select(location_description).limit(20).show()

        val community_area = "Community Area"
        val frequent_area = most_frequent_value(new_data, community_area)

        new_data = new_data.withColumn(
            community_area,
            when(isnan(col(community_area)), lit(frequent_area)).otherwise(col(community_area))
        )

        new_data = new_data.na.drop(Seq("Location"))

        
        /**
        val new_columns = new_data.columns.map(
            col_name => col_name
            .replaceAll("[^a-zA-Z0-9_]", "_")  // Remplace les caractères non valides par "_"
            .replaceAll("^[0-9]", "_")   
        )

        new_data = new_data.toDF(new_columns: _*)

        **/    

        val renamed = new_data.columns.foldLeft(new_data){
            (tmp_df, col_name) => tmp_df.withColumnRenamed(col_name, col_name.replace()@@)
        }
        

        new_data.select(community_area).limit(20).show()

        new_data = clean_invalid_char(new_data, "Location Description")
        new_data = clean_invalid_char(new_data, "Community Area")

        new_data.select("Location Description").limit(20).show()


        

        val transformed_data_file = s"${Util.data_path}/transformed_data"
        Util.save_data(new_data, transformed_data_file)

        spark.stop()
    }
}
```



#### Error stacktrace:

```
scala.collection.LinearSeqOps.apply(LinearSeq.scala:129)
	scala.collection.LinearSeqOps.apply$(LinearSeq.scala:128)
	scala.collection.immutable.List.apply(List.scala:79)
	dotty.tools.dotc.util.Signatures$.applyCallInfo(Signatures.scala:244)
	dotty.tools.dotc.util.Signatures$.computeSignatureHelp(Signatures.scala:101)
	dotty.tools.dotc.util.Signatures$.signatureHelp(Signatures.scala:88)
	dotty.tools.pc.SignatureHelpProvider$.signatureHelp(SignatureHelpProvider.scala:47)
	dotty.tools.pc.ScalaPresentationCompiler.signatureHelp$$anonfun$1(ScalaPresentationCompiler.scala:439)
```
#### Short summary: 

java.lang.IndexOutOfBoundsException: -1
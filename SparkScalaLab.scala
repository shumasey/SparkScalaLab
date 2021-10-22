:require C:/GitHub/SparkScalaLab/spark-xml_2.11-0.12.0.jar
import com.databricks.spark.xml._
val metro=spark.read.format("xml").option("rowTag","item").xml("c:/GitHub/SparkScalaLab/input/*.xml")
//You can take year from "pubDate" column and see what will happen
//val metro1=metro.withColumn("year", year(to_date($"pubDate","EEE, dd MMM yyyy HH:mm:ss ZZZZZ"))-1)
val metro1=metro.withColumn("search",split(col("link"),"/").getItem(6))
val metro2=metro1.withColumn("year",split(col("search"),"-").getItem(8))
metro2.dropDuplicates("dataset:ville").count
metro2.groupBy("year").count().sort("year").show
val maxmin=metro2.rollup("year").agg(sum("dataset:trafic"),avg("dataset:trafic"),max("dataset:trafic"),min("dataset:trafic"))
maxmin.join(metro2, maxmin("max(dataset:trafic)")===metro2("dataset:trafic"), "inner").select(maxmin.col("year"),$"dataset:trafic",$"dataset:station").sort("year").show
maxmin.join(metro2, maxmin("min(dataset:trafic)")===metro2("dataset:trafic"), "inner").select(maxmin.col("year"),$"dataset:trafic",$"dataset:station").sort("year").show
import org.apache.spark.sql.expressions.Window
val position=Window.partitionBy(col("year")).orderBy(col("dataset:trafic").desc)
val metro3=metro2.withColumn("position", rank over position)
metro3.filter($"position"<21).groupBy("position").pivot("year").agg(first("dataset:station")).sort("position").show
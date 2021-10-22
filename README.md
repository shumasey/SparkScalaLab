# SparkScalaLab
 Lab for students how to parse data from open sources and use Spark to make simple analysis with __groupBy(), rollup(), pivot(), agg()__ functions.
 
A lot of data that could be reused for personal purposes is available on the web in the form of html pages or web services. Unfortunately, their raw format is inappropriate and it is necessary to rework them.
RATP, via its OpenData site [https://data.ratp.fr], provides the public with a number of web services allowing access to data such as air quality in stations, real time schedules or even the location of public toilets!
Here we will focus on the evolution of [traffic on the network] (<https://data.ratp.fr/explore/dataset/trafic-annuel-entrant-par-station-du-reseau-ferre-2014/api/>).
The following URL (https://data.ratp.fr/api/records/1.0/search/?dataset=trafic-annuel-entrant-par-station-du-reseau-ferre-2014&rows=1000&format=rss) allows extract the traffic data of all stations in the __xml__ format.
Save this file in your __input__ folder. Do the same for other years up to 2020.

Install Spark and start __spark-shell__. Default Spark doesn't support __xml__, but we can use Databricks utility. Please pay attention on Scala version in Spark to load correct __jar__.
![img](https://github.com/shumasey/SparkScalaLab/blob/main/Screenshots/classpath.png)

If you look through the file you'll see that valuable data are inside _item_ chapter, so we use __item__ as __rowTag__.
![img](https://github.com/shumasey/SparkScalaLab/blob/main/Screenshots/file.png)

![img](https://github.com/shumasey/SparkScalaLab/blob/main/Screenshots/loaddata.png)

It always good to have time in your dataset, here we add column "__year__". We take year from "__link__" column using __split()__ function. As the string has two delimiters "__/__" and "__-__" we use __split()__ twiсe.
![img](https://github.com/shumasey/SparkScalaLab/blob/main/Screenshots/year.png)

Now we can start analysis. How many stations has "métro Parisien" and how many "ville" are covered by metro?
![img](https://github.com/shumasey/SparkScalaLab/blob/main/Screenshots/stations.png)

What amount, average, max and min traffic through years? "__null__" row contains total values.
![img](https://github.com/shumasey/SparkScalaLab/blob/main/Screenshots/traficagg.png)

Which station has max and min traffic?
![img](https://github.com/shumasey/SparkScalaLab/blob/main/Screenshots/maxmin.png)

Finally, list top 20 traffic stations. You can just sort them by year, but let's give this solution a nice look.
Use __Window__ function to rank stations within year
![img](https://github.com/shumasey/SparkScalaLab/blob/main/Screenshots/window.png)

then use __pivot()__ to put them in a table.
![img](https://github.com/shumasey/SparkScalaLab/blob/main/Screenshots/pivot.png)

That's all for today. 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object analysis {
  def main(args: Array[String]) {
    val spark = SparkSession.builder .master("spark://0.0.0.0:7077").appName("Data Application").getOrCreate
    import spark.implicits._

//### 1. Cleanup
//
//    A sample dataset of request logs is given in `data/DataSample.csv`. Consider records that have identical `geoinfo` and `timest` as suspicious. Cleaning up the sample dataset by filtering out those suspicious request records.
//

    val dataFile = "data/DataSample.csv"
    val POIFile = "data/POIList.csv"
    val df = spark.read.format("csv").option("header", "true").load(dataFile).cache()
    val dfPoi  = spark.read.format("csv").option("header", "true").load(POIFile).cache()
    val rdfPoi = dfPoi.withColumn("POILatitude", col(" Latitude").cast("double")).withColumn("POILongitude", col("Longitude").cast("double")).drop(" Latitude").drop("Longitude")
    rdfPoi.createOrReplaceTempView("poidata")
    val filPoiDf = spark.sql("select min(POIID) as POIID, POILatitude,POILongitude from poidata group by POILatitude,POILongitude")
    val rdf = df.withColumnRenamed(" TimeSt","timest").withColumn("Latitude", col("Latitude").cast("double")).withColumn("Longitude", col("Longitude").cast("double"))
    rdf.createOrReplaceTempView("data")
    spark.sql("CACHE table data")
    val filteredDf = spark.sql("select min(_ID) as ID,timest, Country,Province,City,Latitude,Longitude from data  group by timest, Country,Province,City,Latitude,Longitude ").cache
    filteredDf.createOrReplaceTempView("filtereddata")
    filPoiDf.createOrReplaceTempView("filteredpoi")
    spark.sql("CACHE table filtereddata")
    spark.sql("select count(*), 'poifile' from poidata union select count(*), 'datfile' from data union select count(*), 'filtereddata' from filtereddata union select count(*), 'filteredpoi' from filteredpoi ").show()
    spark.sql("UNCACHE table data")
    spark.sql("UNCACHE table filtereddata")
//                           +--------+------------+
//                           |count(1)|     poifile|
//                           +--------+------------+
//                           |       3| filteredpoi|
//                           |   19999|filtereddata|
//                           |       4|     poifile|
//                           |   22025|     datfile|
//                           +--------+------------+



//
//    Assign each _request_ (from `data/DataSample.csv`) to the closest (i.e. minimum distance) _POI_ (from `data/POIList.csv`).
//
//      Note: a _POI_ is a geographical Point of Interest.


    val joinedDf = filteredDf.crossJoin(filPoiDf)
    val filteredDf2 = joinedDf.withColumn("partdistcalc",pow(sin(radians(col("Latitude") - col("POILatitude")) / 2), 2) + cos(radians(col("POILatitude"))) * cos(radians(col("Latitude"))) * pow(sin(radians(col("Longitude") - col("POILongitude")) / 2), 2) ).withColumn("distance", atan2(sqrt(col("partdistcalc")), sqrt(-col("partdistcalc") + 1)) * 12742000)
    filteredDf2.createOrReplaceTempView("distdata")
    spark.sql("CACHE table distdata")

    val LabelDf  = spark.sql("select " +
      "ID,timest,Country,Province,City,Latitude,Longitude,mindistance, POIID, distance" +
      " from distdata inner join (select  ID as IDs,timest as timests,Country as Countrys ,Province as Country2,City as Country3,Latitude as Country4,Longitude as Country5 ,min(distance) as mindistance" +
      " from distdata group by ID,timest,Country,Province,City,Latitude,Longitude ) temp on distdata.ID = temp.IDs and distdata.distance = temp.mindistance")
    LabelDf.createOrReplaceTempView("Labels")
    spark.sql("CACHE table Labels")
    spark.sql("UNCACHE table distdata")
    spark.sql("select POIID,ID,timest,Country,Province,City,Latitude,Longitude,distance from Labels").show()
    spark.sql("select POIID,count(*) as cntID from Labels group by POIID").show()
//                          +-----+-----+
//                          |POIID|cntID|
//                          +-----+-----+
//                          | POI4|  477|
//                          | POI1| 9727|
//                          | POI3| 9795|
//                          +-----+-----+



//
//
//    1. For each _POI_, calculating the average and standard deviation of the distance between the _POI_ to each of its assigned _requests_.

//    spark.sql("select POIID, round(avg(distance)/1000,3) as average_Km, round(stddev(distance)/1000,3) as standard_deviation_Km from Labels group by POIID").show()


//    2. At each _POI_, draw a circle (with the center at the POI) that includes all of its assigned _requests_. Calculate the radius and density (requests/area) for each _POI_.
    val analysisDf = spark.sql("select POIID, round(avg(distance)/1000,3) as average_Km, round(stddev(distance)/1000,3) as standard_deviation_Km, round(max(distance)/1000,3) as radius_Km , " +
      " round(count(*)/pi() * pow((max(distance)/1000),2),2) as density, count(*) as cnt_ID from Labels group by POIID")
    analysisDf.show()

//            +-----+----------+---------------------+---------+------------------+------+
//            |POIID|average_Km|standard_deviation_Km|radius_Km|           density|cnt_ID|
//            +-----+----------+---------------------+---------+------------------+------+
//            | POI4|   497.279|             1472.938| 9349.573|  1.32724787521E10|   477|
//            | POI1|   301.907|               412.43|11531.821|4.1174166423653E11|  9727|
//            | POI3|   451.528|              223.351| 1474.581|   6.77940864283E9|  9795|
//            +-----+----------+---------------------+---------+------------------+------+

//      #### 4a. Model
//
    analysisDf.createOrReplaceTempView("analysis")
    spark.sql("CACHE table analysis")
    spark.sql("UNCACHE table Labels")
    val poiavgDf = spark.sql("select avg (average_Km * cnt_ID) as poiavg from analysis")
    val joinDf = analysisDf.crossJoin(poiavgDf)
    joinDf.createOrReplaceTempView("map")
    spark.sql("CACHE table map")
    spark.sql("select POIID, round(((average_Km * cnt_ID)/poiavg*10)-10,2) as yaxisval from map order by POIID").show()
//                    +-----+--------+
//                    |POIID|yaxisval|
//                    +-----+--------+
//                    | POI1|     1.6|
//                    | POI3|    7.47|
//                    | POI4|   -9.06|
//                    +-----+--------+
    spark.sql("UNCACHE table analysis")
    spark.sql("UNCACHE table map")
    spark.stop()
  }
}

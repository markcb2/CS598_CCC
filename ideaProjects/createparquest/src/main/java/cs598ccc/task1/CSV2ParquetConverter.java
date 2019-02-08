package cs598ccc.task1;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;


public class CSV2ParquetConverter {

    public static void main(String[] args){
        System.out.println("Hello CS598_CCC");
        CSV2ParquetConverter app = new CSV2ParquetConverter();
        app.start();

    }

    public void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Convert CSV to Parquet")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("sep",",")
                .option("dateFormat", "y-M-d")
                .option("nullValue","")
                .load("/tmp/cs598ccc/raw_data/*/*.csv");

        System.out.println("Number of input rows read: " + df.count());

        //removing rows where CRSDeptime or DepTime or CRSArrTime or ArrTime are null
        Dataset<Row> filtered_df = df.where(col("CRSDepTime").isNotNull())
                .where(col("DepTime").isNotNull())
                .where(col("CRSArrTime").isNotNull())
                .where(col("ArrTime").isNotNull())
                .where(col("Origin").isNotNull())
                .where(col("Dest").isNotNull())
                ;


        System.out.println("Number of rows after null values filtered outt: " + filtered_df.count());


        //adding and dropping columns to the dataframe

        System.out.println("Dropping unnecessary columns");

        Dataset<Row> cleansed_df = filtered_df.withColumn("departure", lit(1))
                .withColumn("arrival", lit(1))
                .drop("DistanceGroup")
                .drop("Distance")
                .drop("Quarter")
                .drop("UniqueCarrier")
                .drop("TailNum")
                .drop("OriginCityName")
                .drop("OriginState")
                .drop("OriginStateFips")
                .drop("OriginStateName")
                .drop("OriginWac")
                .drop("DestCityName")
                .drop("DestState")
                .drop("DestStateFips")
                .drop("DestStateName")
                .drop("DestWac")
                .drop("CancellationCode")
                .drop("ArrTimeBlk")
                .drop("DepTimeBlk")
                .drop("DepartureDelayGroups")
                .drop("CRSElapsedTime")
                .drop("ActualElapsedTime")
                .drop("AirTime")
                .drop("flights")
                .drop("CarrierDelay")
                .drop("WeatherDelay")
                .drop("NASDelay")
                .drop("SecurityDelay")
                .drop("LateAircraftDelay")
                .drop("FirstDepTime")
                .drop("TotalAddGTime")
                .drop("LongestAddTime")
                .drop("DivAirportLandings")
                .drop("DivReachedDest")
                .drop("DivActualElapsedTime")
                .drop("DivArrDelay")
                .drop("DivDistance")
                .drop("Div1Airport")
                .drop("Div1AirportID")
                .drop("LongestAddGTime")
                .drop("Div1WheelsOn")
                .drop("Div1TotalGTime")
                .drop("Div1LongestGTime")
                .drop("Div1WheelsOff")
                .drop("Div1TailNum")
                .drop("Div2Airport")
                .drop("Div2WheelsOn")
                .drop("Div2TotalGTime")
                .drop("Div2LongestGTime")
                .drop("Div2WheelsOff")
                .drop("Div2TailNum")
                .drop("ArrivalDelayGroups")
                .drop("_c55")
                .drop("_c75");


        System.out.println("Cast numeric and date fields to the appropriate data type");

        cleansed_df = cleansed_df.withColumn("Year", col("Year").cast(DataTypes.IntegerType))
                .withColumn("Month", col("Month").cast(DataTypes.IntegerType))
                .withColumn("DayofMonth", col("DayofMonth").cast(DataTypes.IntegerType))
                .withColumn("DayOfWeek", col("DayOfWeek").cast(DataTypes.IntegerType))
                .withColumn("FlightDate", col("FlightDate").cast(DataTypes.DateType))
                .withColumn("CRSDepTime", col("CRSDepTime").cast(DataTypes.IntegerType))
                .withColumn("DepTime", col("DepTime").cast(DataTypes.IntegerType))
                .withColumn("DepDelay", col("DepDelay").cast(DataTypes.createDecimalType(10,2)))
                .withColumn("DepDelayMinutes",col("DepDelayMinutes").cast(DataTypes.createDecimalType(10,2)))
                .withColumn("DepDel15",col("DepDel15").cast(DataTypes.IntegerType))
                .withColumn("TaxiOut", col("TaxiOut").cast(DataTypes.createDecimalType(10,2)))
                .withColumn("WheelsOff",col("WheelsOff").cast(DataTypes.IntegerType))
                .withColumn("WheelsOn", col("WheelsOn").cast(DataTypes.IntegerType))
                .withColumn("TaxiIn", col("TaxiIn").cast(DataTypes.createDecimalType(10,2)))
                .withColumn("CRSArrTime", col("CRSArrTime").cast(DataTypes.IntegerType))
                .withColumn("ArrTime", col("ArrTime").cast(DataTypes.IntegerType))
                .withColumn("ArrDelay", col("ArrDelay").cast(DataTypes.createDecimalType(10,2)))
                .withColumn("ArrDelayMinutes", col("ArrDelayMinutes").cast(DataTypes.createDecimalType(10,2)))
                .withColumn("ArrDel15", col("ArrDel15").cast(DataTypes.IntegerType))
                .withColumn("Cancelled", col("Cancelled").cast(DataTypes.IntegerType))
                .withColumn("Diverted", col("Diverted").cast(DataTypes.IntegerType))
                ;





        System.out.println("First 15 rows of cleansed data");
        cleansed_df.show(15);


        System.out.println("Data Schema after cleansing");
        cleansed_df.printSchema();


        System.out.println("Writing data to parquet format");

        cleansed_df.write()
                .format("parquet")
                .mode("overwrite")
                .partitionBy("Year")
                .save("/tmp/cs598ccc/parquet_data/ontimeperf");

        Dataset<Row> parquet_df = spark.read().format("parquet").load("/tmp/cs598ccc/parquet_data/ontimeperf");
        parquet_df.show(7);
        parquet_df.printSchema();
        System.out.println("The parquet dataframe has " + parquet_df.count() + " rows. and " + parquet_df.rdd().getPartitions() + " partitions " );

       Dataset<Row> counts =  parquet_df.selectExpr("count(distinct(origin)) as origin", "count(distinct(Dest)) as dest", "sum(departure) as departure");
       System.out.println("Unique Origins and Destination Airports:" );
       counts.show();


        Dataset<Row> groupedby_df = parquet_df.groupBy("origin", "dest")
                .agg(
                        sum(col("departure")).alias("departure"),
                        sum(col("arrival")).alias("arrival")
                )
                .orderBy(asc("origin"), asc("dest"));

        //groupedby_df.show(1000);

        Dataset<Row> origins_df = parquet_df.groupBy("origin")
                .agg(
                        sum(col("departure")).alias("departure")
                )
                .orderBy(asc("origin"));
        //origins_df.show(1000);
        System.out.println("Number of unique origin airports: " + origins_df.count());


        Dataset<Row> destinations_df = parquet_df.groupBy("dest")
                .agg(
                        sum(col("arrival")).alias("arrival")
                        )
                .orderBy(asc("dest"));
        //destinations_df.show(1000);
        System.out.println("Number of unique destination airports: " + destinations_df.count());

        Column joinExpression = origins_df.col("origin").equalTo(destinations_df.col("dest"));

        String joinType = "inner";

        Dataset<Row> popularAirports2_df = origins_df.join(destinations_df, joinExpression, joinType)
                .select(col("origin").alias("airport"), col("departure").alias("departures"), col("arrival").alias("arrivals"))
                .withColumn("totalArrivalsAndDepartures", expr("(arrivals+departures)"))
                .drop("departures")
                .drop("arrivals")
                .orderBy(desc("totalArrivalsAndDepartures"));
        System.out.println("Airport popularity based on total departures plus arrivals");
        popularAirports2_df.show(300);

        popularAirports2_df.coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save("/tmp/cs598ccc/queryResults/group1Dot1");

    }

}

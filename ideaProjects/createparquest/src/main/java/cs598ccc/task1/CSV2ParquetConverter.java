package cs598ccc.task1;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;


public class CSV2ParquetConverter {

    public static void main(String[] args){
        System.out.println("Hello CS598_CCC");
        CSV2ParquetConverter app = new CSV2ParquetConverter();
        app.start();

    }

    public void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Convert CSV to Parquet")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("sep",",")
                .option("dateFormat", "y-M-d")
                .load("/tmp/cs598ccc/raw_data/*/*.csv");
        System.out.println("Excerpt of the dataframe content:");
        df.show(7, 90);
        System.out.println("Dataframe's schema:");
        df.printSchema();

        //adding and dropping columns to the dataframe

        Dataset<Row> cleansed_df = df.withColumn("departure", lit(1))
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
                .drop("_c75");

        System.out.println("Cleansed The Data");
        cleansed_df.show(7);
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


        parquet_df.groupBy("origin", "dest").count().show();

        parquet_df.groupBy("origin", "dest")
                .agg(sum(col("departure")).alias("departure")).orderBy(desc("departure"))
                .show(50);

        Dataset<Row> groupedby_df = parquet_df.groupBy("origin", "dest")
                .agg(
                        sum(col("departure")).alias("departure"),
                        sum(col("arrival")).alias("arrival")
                )
                .orderBy(asc("origin"), asc("dest"));
        groupedby_df.show(1000);

        Dataset<Row> origins_df = parquet_df.groupBy("origin")
                .agg(
                        sum(col("departure")).alias("departure")
                )
                .orderBy(asc("origin"));
        origins_df.show(1000);
        System.out.println("Number of unique origin airports: " + origins_df.count());


        Dataset<Row> destinations_df = parquet_df.groupBy("dest")
                .agg(
                        sum(col("arrival")).alias("arrival")
                        )
                .orderBy(asc("dest"));
        destinations_df.show(1000);
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

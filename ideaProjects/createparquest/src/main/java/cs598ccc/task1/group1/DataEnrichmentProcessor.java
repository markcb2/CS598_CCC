package cs598ccc.task1.group1;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.log4j.Logger;

public class DataEnrichmentProcessor {

    private static Logger logger = Logger.getLogger(DataEnrichmentProcessor.class);

    public static void main(String[] argss){
        logger.info("Starting DataEnrichmentProcessor");
        DataEnrichmentProcessor app = new DataEnrichmentProcessor();
        app.start();
    }

    public void start(){

        SparkSession spark = SparkSession.builder()
                .appName("Raw Data Enrichment")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");


        logger.info("Reading Parquet Files");

        Dataset<Row> parquet_df = spark.read().format("parquet").load("/tmp/cs598ccc/parquet_data/ontimeperf");
        parquet_df.show(7);
        parquet_df.printSchema();

        logger.info("The parquet dataframe has " + parquet_df.count() + " rows. and " + parquet_df.rdd().getNumPartitions() + " partitions " );

        Dataset<Row> origin_airports_df = parquet_df.groupBy("origin")
                .agg(
                        sum(col("departure")).alias("departure")
                )
                .orderBy(asc("origin"));

        logger.info("Distinct Origin Domestic Airports: " + origin_airports_df.count());

        origin_airports_df.show(400);


        origin_airports_df.write().format("parquet").saveAsTable("origin_airports");


        Dataset<Row> dest_airports_df = parquet_df.groupBy("dest")
                .agg(
                        sum(col("arrival")).alias("arrival")
                )
                .orderBy(asc("dest"));

        logger.info("Distinct Arrival Domestic Airports: " + dest_airports_df.count());

        dest_airports_df.show(400);

        dest_airports_df.write().format("parquet").saveAsTable("dest_airports");


        logger.info("Reading master coordinate data.");

        Dataset<Row> masterCoordintes_df = spark.read().format("csv")
                .option("header", "true")
                .option("sep",",")
                .option("dateFormat", "y-M-d")
                .option("nullValue","")
                .load("/tmp/cs598ccc/ref_data/417923300_T_MASTER_CORD_All_All.csv");



        logger.info("Number of master coordinate data input rows read: " + masterCoordintes_df.count());

        Dataset<Row> usa_airport_long_lat_df = masterCoordintes_df
                .select(col("AIRPORT").alias("AIRPORT"), col("LON_DEGREES").alias("LONGITUDE"), col("LAT_DEGREES").alias("LATITUDE"))
                .distinct()
                .where(col("TR_COUNTRY_NAME").equalTo("United States of America"))
                .orderBy(asc("AIRPORT"))
                ;

        logger.info("Distinct airport long-lat records returned: " + usa_airport_long_lat_df.count());

        usa_airport_long_lat_df.write().format("parquet").saveAsTable("airport_long_lat");

        spark.catalog().listTables().show();

        Dataset<Row> usa_origin_airport_lon_lat_df = spark.sql("SELECT DISTINCT AIRPORT, LONGITUDE, LATITUDE FROM  airport_long_lat WHERE AIRPORT in (SELECT origin FROM origin_airports) ORDER BY AIRPORT ASC");

        logger.info("Distinct usa_origin_airport_lon_lat_df records: " +  usa_origin_airport_lon_lat_df.count());

        usa_origin_airport_lon_lat_df.show(400);

        Dataset<Row> usa_dest_airport_lon_lat_df = spark.sql("SELECT DISTINCT AIRPORT, LONGITUDE, LATITUDE FROM  airport_long_lat WHERE AIRPORT in (SELECT dest FROM dest_airports) ORDER BY AIRPORT ASC");

        logger.info("Distinct usa_dest_airport_lon_lat_df records: " +  usa_dest_airport_lon_lat_df.count());

        usa_dest_airport_lon_lat_df.show(400);

        logger.info("Performing inner Join of parguet_df and usa_origin_airport_lon_lat_df by origin airport code in order to get long and lat for each origin airport ");
        Column joinExpression = parquet_df.col("origin").equalTo(usa_origin_airport_lon_lat_df.col("AIRPORT"));

        String joinType = "inner";

        Dataset<Row> enriched_parquet_df = parquet_df.join(usa_origin_airport_lon_lat_df,joinExpression,joinType)
                .withColumn("LONGITUDE",col("LONGITUDE").cast(DataTypes.createDecimalType(10,4)))
                .withColumnRenamed("LONGITUDE","lon_origin")
                .withColumn("LATITUDE",col("LATITUDE").cast(DataTypes.createDecimalType(10,4)))
                .withColumnRenamed("LATITUDE","lat_origin")
                .drop(col("AIRPORT"))
                .orderBy(desc("origin"))
                ;


        logger.info("Number of rows in enriched_parquet_df is: " + enriched_parquet_df.count());

        enriched_parquet_df.show(50);

        logger.info("Performing inner Join of parguet_df and usa_dest_airport_lon_lat_df by dest airport code in order to get long and lat for each destination airport ");
        Column joinExpression2 = enriched_parquet_df.col("dest").equalTo(usa_dest_airport_lon_lat_df.col("AIRPORT"));

        Dataset<Row> enriched_parquet_2_df = enriched_parquet_df.join(usa_dest_airport_lon_lat_df,joinExpression,joinType)
                .withColumn("LONGITUDE",col("LONGITUDE").cast(DataTypes.createDecimalType(10,4)))
                .withColumnRenamed("LONGITUDE","lon_dest")
                .withColumn("LATITUDE",col("LATITUDE").cast(DataTypes.createDecimalType(10,4)))
                .withColumnRenamed("LATITUDE","lat_dest")
                .drop(col("AIRPORT"))
                .orderBy(desc("FlightDate"), desc("origin"), desc("dest"),desc("Carrier"),desc("FlightNum"))
        ;

        logger.info("Number of rows in enriched_parquet_2_df is: " + enriched_parquet_2_df.count());

        enriched_parquet_2_df.show(50);
        


    }

}

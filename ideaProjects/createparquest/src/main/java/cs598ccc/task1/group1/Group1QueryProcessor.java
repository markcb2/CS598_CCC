package cs598ccc.task1.group1;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.log4j.Logger;

public class Group1QueryProcessor {
    private static Logger logger = Logger.getLogger(Group1QueryProcessor.class);

    public static void main(String[] args){
        logger.info("Starting Group 1 Queries");
        Group1QueryProcessor app = new Group1QueryProcessor();
        app.start();

    }

    public void start() {
        SparkSession spark = SparkSession.builder()
                .appName("CGroup 1 Queries")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        logger.info("Reading Parquet Files");

        Dataset<Row> parquet_df = spark.read().format("parquet").load("/tmp/cs598ccc/parquet_data/ontimeperf");
        parquet_df.show(7);
        parquet_df.printSchema();

        logger.info("The parquet dataframe has " + parquet_df.count() + " rows. and " + parquet_df.rdd().getNumPartitions() + " partitions " );


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

        logger.info("Number of unique origin airports: " + origins_df.count());


        Dataset<Row> destinations_df = parquet_df.groupBy("dest")
                .agg(
                        sum(col("arrival")).alias("arrival")
                )
                .orderBy(asc("dest"));

        //destinations_df.show(1000);

        logger.info("Number of unique destination airports: " + destinations_df.count());

        Column joinExpression = origins_df.col("origin").equalTo(destinations_df.col("dest"));

        String joinType = "inner";

        logger.info("Querying for Top 10 Airports (Departures + Arrivals)");

        Dataset<Row> topTenPopularAirports_df = origins_df.join(destinations_df, joinExpression, joinType)
                .select(col("origin").alias("airport"), col("departure").alias("departures"), col("arrival").alias("arrivals"))
                .withColumn("totalArrivalsAndDepartures", expr("(arrivals+departures)"))
                .drop("departures")
                .drop("arrivals")
                .orderBy(desc("totalArrivalsAndDepartures"))
                .limit(10)
                ;

        System.out.println("Airport popularity based on total departures plus arrivals");
        topTenPopularAirports_df.show();

        logger.info("Saving top 10 airports to hdfs:///tmp/cs598ccc/queryResults/group1Dot1");

        topTenPopularAirports_df.coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save("/tmp/cs598ccc/queryResults/group1Dot1");

    }


}

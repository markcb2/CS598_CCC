package cs598ccc.task1.group3;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Date;

import static org.apache.spark.sql.functions.col;

public class CreateCSVQueryResultsForGroup3 {

    private static Logger logger = Logger.getLogger(Group3dot1QueryProcessor.class);

    public static void main(String[] args){
        logger.info("Starting Group 3 Dot 1 Query");
        CreateCSVQueryResultsForGroup3 app = new CreateCSVQueryResultsForGroup3();
        app.start();

    }

    public void start(){

        SparkSession spark = SparkSession.builder()
                .appName("CSV Creation For Group 2 Query Results")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");


        logger.info("Reading Parquet Files for Query Result for Group 3, Question 2");

        Dataset<Row> query3Dot2FilteredQueryResults_df = spark.read().format("parquet").load("/tmp/cs598ccc/queryResults/group3Dot2_filtered");
        query3Dot2FilteredQueryResults_df.show(7);
        query3Dot2FilteredQueryResults_df.printSchema();

        logger.info("Writing CSV File Output for Query Result for Group 3, Question 2");

        query3Dot2FilteredQueryResults_df.coalesce(12)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save("/tmp/cs598ccc/queryResults/group3Dot2_filtered_csv");

        logger.info("CSV File Output for Query Result for Group 3, Question 2 Has Been Written");


        logger.info("Creating Abridged Version of the Query Result for Group 3, Question 2");

        Dataset<Row> query3Dot2FilteredAndAbridgedQueryResults_df =
                query3Dot2FilteredQueryResults_df.select(col("Leg1_Month"),col("Leg1_Origin"), col("Leg1_Dest"), col("Leg1_Carrier"), col("Leg1_FlightNum"), col("Leg1_FlightDate"), col("Leg1_DepTime")
                , col("Leg1_ArrTime"), col("Leg1_DepDelay"),
                col("Leg2_Origin"), col("Leg2_Dest"), col("Leg2_Carrier"), col("Leg2_FlightNum"), col("Leg2_FlightDate"), col("Leg2_DepTime")
                , col("Leg2_ArrTime"), col("Leg2_DepDelay")

                )
                .where(col("Leg1_Month").isin(1,4,9,12,10))
                .where(col("Leg1_FlightDate").isin(
                        "2008-01-01","2008-01-02",
                        "2008-01-03","2008-01-04","2008-01-05",
                        "2008-04-02","2008-04-03", "2008-04-04",
                        "2008-09-08", "2008-09-09",
                        "2008-10-05",  "2008-10-06",
                        "2008-12-06", "2008-12-07"
                ));


        logger.info("Writing CSV File Output for Abridged Query Result for Group 3, Question 2");

        query3Dot2FilteredAndAbridgedQueryResults_df.coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save("/tmp/cs598ccc/queryResults/group3Dot2_filtered_and_abridged_csv");

        logger.info("CSV File Output for Abridged Query Result for Group 3, Question 2 Has Been Written");


    }
}

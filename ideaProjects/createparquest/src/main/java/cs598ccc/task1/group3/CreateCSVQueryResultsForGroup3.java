package cs598ccc.task1.group3;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
    }
}

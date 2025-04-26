package projet_big_data;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.expressions.Window;

public class SpotifyTopSong2020 {

        public static void main(String[] args) {
                if (args.length < 2) {
                    System.err.println("Usage: SpotifyTopSong2020 <input_path> <output_path>");
                    System.exit(1);
                }
        
                String inputPath = args[0];
                String outputPath = args[1];
        
                SparkSession spark = SparkSession.builder()
                        .appName("SpotifyTopSong2020")
                        .master("local[*]")
                        .getOrCreate();
        
                processSpotifyData(spark, inputPath, outputPath);
        
                spark.stop();
            }
        
            public static void processSpotifyData(SparkSession spark, String inputPath, String outputPath) {
                Dataset<Row> spotifyDF = spark.read()
                        .option("header", "true")
                        .option("inferSchema", "true") // or manually cast streams later if needed
                        .csv(inputPath);
        
                Dataset<Row> spotify2020 = spotifyDF
                        .filter(col("release_date").startsWith("2020"))
                        .filter(col("streams").isNotNull())
                        .withColumn("streams", col("streams").cast("long"));
        
                Dataset<Row> windowedDF = spotify2020
                        .groupBy("country", "track_name")
                        .agg(sum("streams").alias("total_streams"));
        
                WindowSpec window = Window.partitionBy("country").orderBy(col("total_streams").desc());
        
                Dataset<Row> rankedDF = windowedDF
                        .withColumn("rank", row_number().over(window))
                        .filter(col("rank").equalTo(1))
                        .drop("rank");
        
                rankedDF.write()
                        .option("header", "true")
                        .mode("overwrite")
                        .csv(outputPath);
            }
}

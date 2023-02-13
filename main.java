import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class EffectiveDiameter {
    public static void main(String[] args) {
        // Initialize Spark configuration and context
        SparkConf conf = new SparkConf().setAppName("EffectiveDiameter");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the citation edges and published dates into RDDs
        JavaRDD<String> citationEdges = sc.textFile("citations.txt");
        JavaRDD<String> publishedDates = sc.textFile("published-dates.txt");

        // Create a pair RDD of the citation edges with the publication year as the key
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> edgesByYear = citationEdges.flatMapToPair(line -> {
            String[] fields = line.split(" ");
            int fromNode = Integer.parseInt(fields[0]);
            int toNode = Integer.parseInt(fields[1]);
            List<Tuple2<Integer, Tuple2<Integer, Integer>>> results = new ArrayList<>();
            for (int year = 1993; year <= 2003; year++) {
                results.add(new Tuple2<>(year, new Tuple2<>(fromNode, toNode)));
            }
            return results.iterator();
        });

        // Create a pair RDD of the publication year and publication date for each vertex
        JavaPairRDD<Integer, Integer> publicationDates = publishedDates.flatMapToPair(line -> {
            String[] fields = line.split(" ");
            int node = Integer.parseInt(fields[0]);
            int publicationYear = Integer.parseInt(fields[1]);
            List<Tuple2<Integer, Integer>> results = new ArrayList<>();
            for (int year = 1993; year <= 2003; year++) {
                results.add(new Tuple2<>(year, node));
            }
            return results.iterator();
        });

        // Calculate the effective diameter for each year
        for (int year = 1993; year <= 2003; year++) {
            // Filter the edges and publication dates for the current year
            JavaPairRDD<Integer, Tuple2<Integer, Integer>> yearEdges = edgesByYear.filter(edge -> edge._1 == year);
            JavaPairRDD<Integer, Integer> yearPublicationDates = publicationDates.filter(node -> node._1 == year);

            // Calculate the shortest paths between all node pairs
            
            // I am stuck here, have to implement BFS 
            
            // Calculate the effective diameter for the current year
        }
    }
}

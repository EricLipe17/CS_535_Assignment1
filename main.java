import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Main() {
        public static void main(String[] args)  {
        SparkSession spark = SparkSession
                .builder()
                .appName("Assignment1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        SQLContext sqlContext = new SQLContext(spark);

                // Create spark structs to represent columns for dataframes
        StructType vertex_df_cols = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("published_date", DataTypes.StringType, true)
        });

        StructType edge_df_cols = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("src", DataTypes.StringType, true),
                DataTypes.createStructField("dst", DataTypes.StringType, true)
        });

        Dataset<Row> edge_df = sqlContext.read().option("header","true").option("sep", "\t").schema(edge_df_cols)
                                            .csv("hdfs:///citations.txt");
        edge_df = edge_df.filter(not(edge_df.col("src").contains("#")));
        Dataset<Row> edge_src = edge_df.select("src").distinct().withColumnRenamed("src", "id");
        Dataset<Row> edge_dst = edge_df.select("dst").distinct().withColumnRenamed("dst", "id");
        Dataset<Row> vertex_df = edge_src.union(edge_dst).distinct().withColumnRenamed("id", "id_old");

        Dataset<Row> properties = sqlContext.read().option("header","true").option("sep", "\t").schema(vertex_df_cols)
                .csv("hdfs:///published-dates.txt");
        properties = properties.withColumn("published_year", properties.col("published_date").substr(0,4)).drop("published_date");

        vertex_df = properties.join(vertex_df, properties.col("id").equalTo(vertex_df.col("id_old"))).drop("id_old");

        // Create data containers
        ArrayList<ArrayList<Long>> vertices_edges_by_year = new ArrayList<>();
        ArrayList<ArrayList<Long>> paths_by_year = new ArrayList<>();
        long num_vertices;
        long num_edges;
            
        // Execute algorithms on each subgraph
        for (long year = 1993; year < 2003; year++) {
            // Collect number of vertices for this subgraph
            Dataset<Row> vertices_by_year_df = vertex_df.filter(vertex_df.col("published_year").$less$eq(year));
            num_vertices = vertices_by_year_df.count();

            // Collect number of out edges for this subgraph
            Dataset<Row> edges_by_year = vertices_by_year_df.join(edge_df, vertices_by_year_df.col("id").equalTo(edge_df.col("src")), "inner");
            num_edges = (long) edges_by_year.groupBy("src").count().select(sum("count")).collectAsList().get(0).get(0);

            // Evaluate g(d) where 1 <= d <= 4
            ArrayList<Long> distribution = new ArrayList<>(4);
            distribution.add(vertices_by_year_df.count());
            Dataset<Row> dst = edges_by_year.select("dst").withColumnRenamed("dst", "dst_original").dropDuplicates();
            for (int i = 0; i < 3; i++) {
                Dataset<Row> new_src = dst.alias("dst").join(edges_by_year.alias("edges"), col("dst.dst_original").equalTo(col("edges.src")), "inner").drop("dst_original");
                dst = new_src.select("dst").withColumnRenamed("dst", "dst_original").dropDuplicates();
                distribution.add(dst.distinct().count());
            }

         // Save vertex and edge counts for this subgraph
         ArrayList<Long> vertices_edges = new ArrayList<>(2);
         vertices_edges.add(num_vertices);
         vertices_edges.add(num_edges);
         vertices_edges_by_year.add(vertices_edges);

         // Save distribution for this subgraph
         paths_by_year.add(distribution);
        }

        // Determine total number of connected node pairs
        ArrayList<Long> connected_node_pairs = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            long count = 0;
            for (ArrayList<Long> paths: paths_by_year) {
                count += paths.get(i);
            }
            connected_node_pairs.add(count);
        }
        long total_connected_node_pairs = connected_node_pairs.stream().mapToLong(Long::longValue).sum();

        // Calculate effective diameter for each year
        for (int i = 0; i < 9; i++) {
            long cumulative_count = 0;
            int lower_bound = i + 1;
            int upper_bound = i + 2;
            long d_lower = lower_bound;
            long d_upper = upper_bound;
            long g_lower = connected_node_pairs.get(i);
            long g_upper = connected_node_pairs.get(i + 1);
            for (ArrayList<Long> paths: paths_by_year) {
                cumulative_count += paths.get(i);
            }
            while (d_upper <= 4 && cumulative_count / (double)total_connected_node_pairs < 0.9) {
                cumulative_count += connected_node_pairs.get(upper_bound - 1);
                upper_bound++;
                d_upper++;
                g_upper = connected_node_pairs.get(upper_bound - 1);
            }
            double x = (cumulative_count / (double)total_connected_node_pairs - (g_upper / (double)total_connected_node_pairs)) /
                    ((g_lower / (double)total_connected_node_pairs) - (g_upper / (double)total_connected_node_pairs));
            double effective_diameter = d_lower + x;
            System.out.println("Effective diameter for year " + (i + 1993) + ": " + effective_diameter);
        }//end for loop
    }//end function main
}// end class Main

package org.ass1;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Main {
    public static void main(String[] args)  {
        Path edge_path = Paths.get(args[0]);
        Path vertices_path = Paths.get(args[1]);

        // Get all edge and vertices
        ArrayList<Row> edges = new ArrayList<>();
        HashMap<String, String> vertices_map = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(edge_path.toAbsolutePath().toString()))) {
            String line;
            String[] edge;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("#"))
                    continue;
                edge = line.split("\t");
                edges.add(RowFactory.create(edge));
                vertices_map.put(edge[0], "");
                vertices_map.put(edge[1], "");
            }
        } catch (IOException e) {
            System.out.print("Caught IOException: ");
            System.out.println(e.getMessage());
        }

        // Create vertices with properties
        try (BufferedReader br = new BufferedReader(new FileReader(vertices_path.toAbsolutePath().toString()))) {
            String line;
            String[] vertex_prop;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("#"))
                    continue;

                vertex_prop = line.split("\t");
                if (vertex_prop[0].startsWith("11"))
                    vertex_prop[0] = vertex_prop[0].substring(2);

                vertex_prop[1] = vertex_prop[1].substring(0, 4);
                if (vertices_map.containsKey(vertex_prop[0]))
                    vertices_map.put(vertex_prop[0], vertex_prop[1]);
            }
        } catch (IOException e) {
            System.out.print("Caught IOException: ");
            System.out.println(e.getMessage());
        }

        // Map vertices to spark rows
        List<Row> vertices = new ArrayList<>();
        for (Map.Entry<String, String> entry : vertices_map.entrySet()) {
            vertices.add(RowFactory.create(entry.getKey(), entry.getValue()));
        }

        // Create the spark context
        SparkSession spark = SparkSession
                .builder()
                .appName("Assignment1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        SQLContext sqlContext = new SQLContext(spark);

        // Create spark structs to represent columns for dataframes
        StructType vertex_df_cols = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("published_year", DataTypes.StringType, true)
        });

        StructType edge_df_cols = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("src", DataTypes.StringType, true),
                DataTypes.createStructField("dst", DataTypes.StringType, true)
        });


        // Create a Vertex DataFrame with unique ID column "id"
        Dataset<Row> vertex_df = sqlContext.createDataFrame(vertices, vertex_df_cols);
        // Create an Edge DataFrame with "src" and "dst" columns
        Dataset<Row> edge_df = sqlContext.createDataFrame(edges, edge_df_cols);

        // Filter out vertices that don't have a published date
        vertex_df = vertex_df.filter("published_year != \"\"");

        // Note don't need to filter out edges since the algorithms employed below do that already

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

            // Store data
            ArrayList<Long> vals = new ArrayList<>();
            vals.add(year);
            vals.add(num_vertices);
            vals.add(num_edges);

            vertices_edges_by_year.add(vals);
            paths_by_year.add(distribution);
        }

        System.out.println(vertex_df.count());
        System.out.println(edge_df.count());
        System.out.println(vertices_edges_by_year);
        System.out.println(paths_by_year);
    }
}

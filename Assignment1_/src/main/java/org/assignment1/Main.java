package org.assignment1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args)  {
        Path edge_path = Paths.get("citations.txt");
        Path vertices_path = Paths.get("published-dates.txt");
        List<Integer> years = IntStream.range(1993, 2003).boxed().toList();

        // Get all edge and vertices
        ArrayList<String[]> edges = new ArrayList<>();
        HashMap<String, String> vertices_map = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(edge_path.toAbsolutePath().toString()))) {
            String line;
            String[] edge;
            String[] complete_edge = new String[3];
            while ((line = br.readLine()) != null) {
                if (line.startsWith("#"))
                    continue;
               edge = line.split("\t");
               complete_edge[0] = edge[0];
               complete_edge[1] = edge[1];
               complete_edge[2] = "cites";
               edges.add(complete_edge);
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

        List<String[]> vertices = new ArrayList<>();
        for (Map.Entry<String, String> entry : vertices_map.entrySet()) {
            vertices.add(new String[]{entry.getKey(), entry.getValue()});
        }


        // Create the spark context
        SparkSession spark = SparkSession
                .builder()
                .appName("Assignment1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        SQLContext sqlContext = new SQLContext(spark);

        StructType vertex_df_cols = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("published_year", DataTypes.StringType, true)
        });

        StructType edge_df_cols = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("src", DataTypes.StringType, true),
                DataTypes.createStructField("dst", DataTypes.StringType, true),
                DataTypes.createStructField("relationship", DataTypes.StringType, true)
        });

        // Create a Vertex DataFrame with unique ID column "id"
        Dataset<Row> vertex_df = sqlContext.createDataFrame(vertices, vertex_df_cols.getClass());
        // Create an Edge DataFrame with "src" and "dst" columns
        Dataset<Row> edge_df = sqlContext.createDataFrame(edges, edge_df_cols.getClass());

        // Collect data
        ArrayList<Long[]> data = new ArrayList<>();
        long num_vertices = 0;
        long num_edges = 0;

        for (long year : years) {
            // Collect number of vertices by year
            Dataset<Row> vertices_by_year_df = vertex_df.filter(vertex_df.col("published_year").$less$eq(year));
            vertices_by_year_df.show(10);
            num_vertices = vertices_by_year_df.count();

            // Collect number of out edges by year
            Dataset<Row> edges_by_year = vertices_by_year_df.join(edge_df, vertices_by_year_df.col("id").equalTo(edge_df.col("src")), "inner");
            edges_by_year.groupBy("src").count().select(sum("count")).show(10);

            // Store data
            data.add(new Long[]{year, num_vertices, num_edges});
        }

        System.out.println(vertex_df.count());
        System.out.println(edge_df.count());
        System.out.println(data);
    }
}

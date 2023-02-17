package org.ass1;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

public class Main {
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

        // Execute algorithms on each subgraph
        for (long year = 1993; year < 2003; year++) {
            // Collect number of vertices for this subgraph
            Dataset<Row> vertices_by_year_df = vertex_df.filter(vertex_df.col("published_year").$less$eq(year));
            Dataset<Row> num_verts = vertices_by_year_df.groupBy("published_year").count().select(sum("count")).withColumnRenamed("sum(count)", "num_vertices");
            num_verts = num_verts.withColumn("row_id", row_number().over(Window.orderBy(lit(1))));

            // Collect number of out edges for this subgraph
            Dataset<Row> edges_by_year = vertices_by_year_df.join(edge_df, vertices_by_year_df.col("id").equalTo(edge_df.col("src")), "inner");
            Dataset<Row> out_edges = edges_by_year.groupBy("src").count().select(sum("count")).withColumnRenamed("sum(count)", "num_out_edges");
            out_edges = out_edges.withColumn("row_id", row_number().over(Window.orderBy(lit(1))));

            // Write data
            Dataset<Row> v_e_data = num_verts.join(out_edges, num_verts.col("row_id").equalTo(out_edges.col("row_id")), "inner").drop("row_id");
            v_e_data.coalesce(1).write().mode(SaveMode.Overwrite).option("header", true).csv(String.format("/output/numVerts_numOutEdges_%d.csv", year));

            // Evaluate g(d) where 1 <= d <= 4
            Dataset<Row> dst = edges_by_year.select("dst").withColumnRenamed("dst", "dst_original");
            Dataset<Row> summed_dst = dst.groupBy("dst_original").count().select(sum("count"));
            summed_dst.coalesce(1).write().mode(SaveMode.Overwrite).option("header", true).csv(String.format("/output/num_paths_g1_%d.csv", year));
            for (int i = 2; i < 5; i++) {
                Dataset<Row> new_src = dst.alias("dst").join(edges_by_year.alias("edges"), col("dst.dst_original").equalTo(col("edges.src")), "inner").drop("dst_original");
                dst = new_src.select("dst").withColumnRenamed("dst", "dst_original");
                summed_dst = dst.groupBy("dst_original").count().select(sum("count"));
                summed_dst.coalesce(1).write().mode(SaveMode.Overwrite).option("header", true).csv(String.format("/output/num_paths_g%d_%d.csv", i, year));
            }
        }
    }
}

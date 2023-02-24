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
            

            // Calculate effective diameter
            int maxDistance = -1;
            for (long year = 1993; year < 2003; year++) {
                Dataset<Row> subgraph_vertices = vertex_df.filter(vertex_df.col("published_year").$less$eq(year));
                Dataset<Row> subgraph_edges = subgraph_vertices.join(edge_df, subgraph_vertices.col("id").equalTo(edge_df.col("src")), "inner");

                // Generate all pairs of vertices
                Dataset<Row> vertex_pairs = subgraph_vertices.crossJoin(subgraph_vertices)
                        .filter(col("id").notEqual(col("id1")))
                        .withColumnRenamed("id", "source")
                        .withColumnRenamed("published_year", "source_year")
                        .withColumnRenamed("id1", "destination")
                        .withColumnRenamed("published_year1", "destination_year");

                // Calculate distances for each vertex pair
                Dataset<Row> distances = vertex_pairs.join(subgraph_edges, vertex_pairs.col("source").equalTo(subgraph_edges.col("src"))
                        .and(vertex_pairs.col("destination").equalTo(subgraph_edges.col("dst"))), "left")
                        .withColumn("distance", when(col("src").isNull(), lit(Integer.MAX_VALUE)).otherwise(lit(1)))
                        .groupBy("source", "source_year", "destination", "destination_year")
                        .agg(min("distance").alias("distance"))
                        .cache();

                // Find max distance for this subgraph
                int subgraphMaxDistance = distances.select(max("distance")).first().getInt(0);

                if (subgraphMaxDistance > maxDistance) {
                    maxDistance = subgraphMaxDistance;
                }
            }

            // Output max distance
            System.out.println("Effective diameter: " + maxDistance);

}

                
            

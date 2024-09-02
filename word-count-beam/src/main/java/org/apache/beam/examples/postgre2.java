package org.apache.beam.examples;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class postgre2 {

    public static void main(String[] args) {
        // Create the pipeline options
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        // SQL statement to select rows from the table
        String sql ="select id,name from public.Person";

        // Create a `JdbcIO` source to read from the table
        JdbcIO.Read<Map<String, Object>> tableSource = JdbcIO.<Map<String, Object>>read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                "org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres")
                        .withUsername("")
                        .withPassword("")
                )
                .withQuery(sql)
                .withRowMapper(new JdbcIO.RowMapper<Map<String, Object>>() {
                    @Override
                    public Map<String, Object> mapRow(ResultSet resultSet) throws Exception {
                        // Create a map of column names and values
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getString(1));
                        map.put("name", resultSet.getString(2));
                        return map;
                    }
                });

        // Use the source in a pipeline
        Pipeline p = Pipeline.create(options);
        PCollection<Map<String, Object>> rows = p.apply(tableSource);

        // Write the rows to a CSV file
        rows.apply(MapElements.via(new SimpleFunction<Map<String, Object>, String>() {
                    @Override
                    public String apply(Map<String, Object> input) {
                        // Extract the values from the map and convert them to strings
                        String id = input.get("id").toString();
                        String name = input.get("name").toString();

                        // Concatenate the values with a comma delimiter
                        return String.join(",", id, name);
                    }
                }))
                .apply(TextIO.write().to("/Users/darviddree/IdeaProjects/word-count-beam/target/test.csv"));



        p.run().waitUntilFinish();
    }
}

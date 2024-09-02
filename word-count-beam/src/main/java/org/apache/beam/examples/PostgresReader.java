package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.PCollection;

import java.sql.ResultSet;

public class PostgresReader {
    public static void main(String[] args) {
        // Parse the command-line options
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);


        // Read the data from the table
        PCollection<KV<Integer, String>> store = pipeline.apply(JdbcIO.<KV<Integer, String>>read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                "org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres")
                        .withUsername("")
                        .withPassword(""))
                .withQuery("select id,name from public.Person")
                .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of()))
                .withRowMapper(new JdbcIO.RowMapper<KV<Integer, String>>() {
                    public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
                        return KV.of(resultSet.getInt(1), resultSet.getString(2));
                    }
                })
        );

        store.apply(MapElements.via(new SimpleFunction<KV<Integer, String>, String>() {
            public String apply(KV<Integer, String> input) {
                return input.getKey() + "," + input.getValue();
            }
        })).apply(TextIO.write().to("/Users/darviddree/IdeaProjects/word-count-beam/target/output.csv"));

     pipeline.run();
    }
}

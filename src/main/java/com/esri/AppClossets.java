package com.esri;

import com.pervasive.datarush.graphs.EngineConfig;
import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphFactory;
import com.pervasive.datarush.io.WriteMode;
import com.pervasive.datarush.operators.io.textfile.ReadDelimitedText;
import com.pervasive.datarush.operators.io.textfile.WriteDelimitedText;
import com.pervasive.datarush.schema.SchemaBuilder;
import com.pervasive.datarush.schema.TextRecord;

public class AppClossets
{
    public static void main(String[] args)
    {
        final LogicalGraph graph = LogicalGraphFactory.newLogicalGraph("Clossest");

        final TextRecord inputSchema = SchemaBuilder.define(
                SchemaBuilder.DOUBLE("LON"),
                SchemaBuilder.DOUBLE("LAT")
        );

        final ReadDelimitedText input = graph.add(new ReadDelimitedText("hdfs://h0:8020/user/root/input/*"));
        // final ReadDelimitedText input = graph.add(new ReadDelimitedText("/tmp/input.tsv"));
        input.setFieldSeparator("\t");
        input.setHeader(false);
        input.setSchema(inputSchema);

        final TextRecord lookupSchema = SchemaBuilder.define(
                SchemaBuilder.DOUBLE("LON"),
                SchemaBuilder.DOUBLE("LAT"),
                SchemaBuilder.STRING("ATTRIBUTE")
        );

        final ReadDelimitedText lookup = graph.add(new ReadDelimitedText("hdfs://h0:8020/user/root/lookup/*"));
        // final ReadDelimitedText lookup = graph.add(new ReadDelimitedText("/tmp/lookup.tsv"));
        lookup.setFieldSeparator("\t");
        lookup.setHeader(false);
        lookup.setSchema(lookupSchema);

        final WriteDelimitedText output = graph.add(new WriteDelimitedText("/tmp/output.tsv", WriteMode.OVERWRITE));
        output.setWriteOnClient(true);
        output.setHeader(true);
        output.setFieldDelimiter("");
        output.setFieldSeparator("\t");
        output.setWriteSingleSink(true);

        final LookupGeodesticDistanceOnWGS84Operator operator = graph.add(new LookupGeodesticDistanceOnWGS84Operator());
        graph.connect(input.getOutput(), operator.getInput());
        graph.connect(lookup.getOutput(), operator.getLookup());
        graph.connect(operator.getOutput(), output.getInput());

        // runInJVM(graph);

        runOnCluster(graph);
    }

    private static void runInJVM(final LogicalGraph graph)
    {
        graph.run();
    }

    private static void runOnCluster(final LogicalGraph graph)
    {
        final EngineConfig engineConfig = EngineConfig.engine().
                parallelism(0).
                monitored(true).
                cluster("dr://h0:1099");

        graph.run(engineConfig);
    }
}

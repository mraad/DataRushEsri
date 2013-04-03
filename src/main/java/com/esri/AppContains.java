package com.esri;

import com.pervasive.datarush.graphs.EngineConfig;
import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphFactory;
import com.pervasive.datarush.io.WriteMode;
import com.pervasive.datarush.operators.io.textfile.ReadDelimitedText;
import com.pervasive.datarush.operators.io.textfile.WriteDelimitedText;
import com.pervasive.datarush.schema.SchemaBuilder;
import com.pervasive.datarush.schema.TextRecord;

public class AppContains
{
    public static void main(String[] args)
    {
        final LogicalGraph graph = LogicalGraphFactory.newLogicalGraph("Contains");

        final TextRecord inputSchema = SchemaBuilder.define(
                SchemaBuilder.DOUBLE("LON"),
                SchemaBuilder.DOUBLE("LAT")
        );

        final ReadDelimitedText input = graph.add(new ReadDelimitedText("hdfs://h0:8020/user/root/input/*"));
        // final ReadDelimitedText input = graph.add(new ReadDelimitedText("/tmp/input.tsv"));
        input.setFieldSeparator("\t");
        input.setHeader(false);
        input.setSchema(inputSchema);

        final WriteDelimitedText output = graph.add(new WriteDelimitedText("/tmp/output.tsv", WriteMode.OVERWRITE));
        output.setWriteOnClient(true);
        output.setHeader(true);
        output.setFieldDelimiter("");
        output.setFieldSeparator("\t");
        output.setWriteSingleSink(true);

        final ContainsOperator operator = graph.add(new ContainsOperator());
        operator.setLonFieldName("LON");
        operator.setLatFieldName("LAT");
        final EsriPolygon polygon = new EsriPolygon(new EsriSpatialReference(4326));
        final EsriRing ring = new EsriRing(new EsriCoords[]{
                new EsriCoords(0, 0),
                new EsriCoords(180, 0),
                new EsriCoords(180, 90),
                new EsriCoords(0, 90)
        });
        polygon.setRings(new EsriRing[]{ring});
        operator.setPolygon(polygon);

        graph.connect(input.getOutput(), operator.getInput());
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

package com.esri;

import com.pervasive.datarush.graphs.EngineConfig;
import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphFactory;
import com.pervasive.datarush.io.WriteMode;
import com.pervasive.datarush.operators.io.textfile.ReadDelimitedText;
import com.pervasive.datarush.operators.io.textfile.WriteDelimitedText;
import com.pervasive.datarush.schema.SchemaBuilder;
import com.pervasive.datarush.schema.TextRecord;

public class AppLookupContains
{
    public static void main(String[] args)
    {
        final boolean local = args.length == 0;

        final LogicalGraph graph = LogicalGraphFactory.newLogicalGraph("AppLookupContains");

        final TextRecord inputSchema = SchemaBuilder.define(
                SchemaBuilder.DOUBLE("LON"),
                SchemaBuilder.DOUBLE("LAT")
        );

        final String pointPath = local ? "/tmp/points.txt" : "hdfs://h0:8020/user/root/points/points.txt";
        final ReadDelimitedText input = graph.add(new ReadDelimitedText(pointPath));
        input.setFieldDelimiter("");
        input.setFieldSeparator("\t");
        input.setHeader(true);
        input.setSchema(inputSchema);

        final TextRecord lookupSchema = SchemaBuilder.define(
                SchemaBuilder.STRING("SHAPE"),
                SchemaBuilder.STRING("ATTRIBUTE")
        );

        final String polygonPath = local ? "/tmp/polygons.txt" : "hdfs://h0:8020/user/root/polygons/polygons.txt";
        final ReadDelimitedText polygons = graph.add(new ReadDelimitedText(polygonPath));
        polygons.setFieldSeparator("\t");
        polygons.setHeader(false);
        polygons.setSchema(lookupSchema);

        final WriteDelimitedText output = graph.add(new WriteDelimitedText("/tmp/output.txt", WriteMode.OVERWRITE));
        output.setWriteSingleSink(true);
        output.setWriteOnClient(true);
        output.setHeader(true);
        output.setFieldDelimiter("");
        output.setFieldSeparator("\t");

        final LookupContainsOperator operator = graph.add(new LookupContainsOperator());

        graph.connect(input.getOutput(), operator.getInput());
        graph.connect(polygons.getOutput(), operator.getLookup());
        graph.connect(operator.getOutput(), output.getInput());

        if (local)
        {
            graph.run();
        }
        else
        {
            graph.run(EngineConfig.engine().
                    parallelism(0).
                    monitored(true).
                    cluster("dr://h0:1099"));
        }
    }
}

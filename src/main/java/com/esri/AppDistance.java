package com.esri;

import com.pervasive.datarush.graphs.EngineConfig;
import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphFactory;
import com.pervasive.datarush.operators.io.textfile.ReadDelimitedText;
import com.pervasive.datarush.operators.sink.LogRows;
import com.pervasive.datarush.schema.SchemaBuilder;
import com.pervasive.datarush.schema.TextRecord;

public final class AppDistance
{
    public static void main(String[] args)
    {
        final boolean local = args.length == 0;

        final LogicalGraph graph = LogicalGraphFactory.newLogicalGraph("AppDistance");

        final TextRecord schema = SchemaBuilder.define(
                SchemaBuilder.DOUBLE("LON"),
                SchemaBuilder.DOUBLE("LAT")
        );

        final String path = local ? "/tmp/points.txt" : "hdfs://h0:8020/user/root/points/*";
        final ReadDelimitedText reader = graph.add(new ReadDelimitedText(path));
        reader.setFieldSeparator("\t");
        reader.setHeader(true);
        reader.setSchema(schema);

        final DistanceOperator distanceOperator = graph.add(new DistanceOperator());
        distanceOperator.setLat(0.0);
        distanceOperator.setLon(0.0);
        distanceOperator.setLonFieldName("LON");
        distanceOperator.setLatFieldName("LAT");

        final LogRows logRows = graph.add(new LogRows(1));

        graph.connect(reader.getOutput(), distanceOperator.getInput());
        graph.connect(distanceOperator.getOutput(), logRows.getInput());

        if (local)
        {
            graph.run();
        }
        else
        {
            final EngineConfig engineConfig = EngineConfig.engine().
                    parallelism(0).
                    monitored(false).
                    cluster("dr://h0:1099");
            graph.run(engineConfig);
        }
    }
}

package com.esri;

import com.pervasive.datarush.graphs.EngineConfig;
import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.json.JSON;
import com.pervasive.datarush.util.FileUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class App
{
    public static void main(String[] args) throws IOException
    {
        if (args.length != 4)
        {
            System.err.println("Usage: " + App.class + " workflow.dr workflow.properties num-parallelism local|dr://dr-host:1099");
            return;
        }

        final JSON json = new JSON();

        // Load an exported KNIME workflow
        final String text = FileUtil.readFileString(new File(args[0]), Charset.defaultCharset());
        final LogicalGraph graph = json.parse(text, LogicalGraph.class);

        final Properties properties = new Properties();
        final InputStream inputStream = new FileInputStream(args[1]);
        try
        {
            properties.load(inputStream);
        }
        finally
        {
            inputStream.close();
        }

        // Update the variables of the graph from a properties file
        final Set<Map.Entry<Object, Object>> entries = properties.entrySet();
        for (final Map.Entry<Object, Object> entry : entries)
        {
            graph.setProperty(entry.getKey().toString(), entry.getValue());
        }

        final int parallelism = Integer.parseInt(args[2]);

        final String url = args[3];
        if ("local".equalsIgnoreCase(url))
        {
            graph.run();
        }
        else
        {
            graph.run(EngineConfig.engine().
                    parallelism(parallelism).
                    monitored(false).
                    cluster(url));
        }
    }
}

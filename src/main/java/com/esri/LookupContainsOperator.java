package com.esri;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Point;
import com.pervasive.datarush.operators.CompositeOperator;
import com.pervasive.datarush.operators.CompositionContext;
import com.pervasive.datarush.operators.ExecutableOperator;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.ParallelismStrategy;
import com.pervasive.datarush.operators.RecordPipelineOperator;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.ports.model.SimpleModelPort;
import com.pervasive.datarush.ports.physical.DoubleInputField;
import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.physical.RecordOutput;
import com.pervasive.datarush.ports.physical.ScalarInputField;
import com.pervasive.datarush.ports.physical.ScalarOutputField;
import com.pervasive.datarush.ports.physical.StringInputField;
import com.pervasive.datarush.ports.physical.StringOutputField;
import com.pervasive.datarush.ports.record.RecordPort;
import com.pervasive.datarush.types.Field;
import com.pervasive.datarush.types.RecordTokenTypeBuilder;
import com.pervasive.datarush.types.TokenTypeConstant;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.annotate.JsonCreator;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

/**
 */
public final class LookupContainsOperator extends CompositeOperator implements RecordPipelineOperator
{
    private final Log m_log = LogFactory.getLog(LookupContainsOperator.class);

    static final class Feature implements Serializable
    {
        private MapGeometry m_mapGeometry;
        private String m_attribute;

        @JsonCreator
        Feature()
        {
        }

        public String getAttribute()
        {
            return m_attribute;
        }

        public void setAttribute(final String attribute)
        {
            this.m_attribute = attribute;
        }

        public MapGeometry getMapGeometry()
        {
            return m_mapGeometry;
        }

        public void setMapGeometry(final MapGeometry mapGeometry)
        {
            m_mapGeometry = mapGeometry;
        }
    }

    private final class Builder extends ExecutableOperator
    {
        final RecordPort input = newRecordInput("lookup");
        final SimpleModelPort<ArrayList> model = newOutput("model",
                new SimpleModelPort.Factory<ArrayList>(ArrayList.class));

        @JsonCreator
        private Builder()
        {
        }

        @Override
        protected void computeMetadata(final StreamingMetadataContext context)
        {
            context.parallelize(ParallelismStrategy.NON_PARALLELIZABLE);
        }

        @Override
        protected void execute(final ExecutionContext context)
        {
            final JsonFactory jsonFactory = new JsonFactory();
            final ArrayList arrayList = new ArrayList();
            final RecordInput recordInput = input.getInput(context);
            final StringInputField geomField = (StringInputField) recordInput.getField(m_geometryField);
            final StringInputField attrField = (StringInputField) recordInput.getField(m_attributeField);

            final ScalarInputField scalarInputField = recordInput.getField(m_attributeField);
            scalarInputField.getName();
            scalarInputField.getType();

            while (recordInput.stepNext())
            {
                final String geom = geomField.asString();
                final JsonParser jsonParser;
                try
                {
                    jsonParser = jsonFactory.createJsonParser(geom);
                }
                catch (IOException e)
                {
                    m_log.warn("Cannot JSON parse " + geom);
                    continue;
                }
                final Feature feature = new Feature();
                feature.setMapGeometry(GeometryEngine.jsonToGeometry(jsonParser));
                feature.setAttribute(attrField.asString());
                arrayList.add(feature);
            }
            model.setModel(context, arrayList);
        }
    }

    private final class Walker extends ExecutableOperator
    {
        final RecordPort input = newRecordInput("input");
        final SimpleModelPort<ArrayList> model = newInput("model",
                new SimpleModelPort.Factory<ArrayList>(ArrayList.class));
        final RecordPort output = newRecordOutput("output");

        @JsonCreator
        private Walker()
        {
        }

        @Override
        protected void computeMetadata(final StreamingMetadataContext context)
        {
            context.parallelize(ParallelismStrategy.NEGOTIATE_BASED_ON_SOURCE);

            final RecordTokenTypeBuilder builder = new RecordTokenTypeBuilder();
            for (Field field : input.getType(context))
            {
                builder.addField(field);
            }

            builder.addField(m_attributeField, TokenTypeConstant.STRING);

            output.setType(context, builder.toType());

            output.setOutputDataOrdering(context,
                    input.getSourceDataOrdering(context));

            output.setOutputDataDistribution(context,
                    input.getSourceDataDistribution(context));

        }

        @Override
        protected void execute(final ExecutionContext context)
        {
            final ArrayList featureList = model.getModel(context);

            final RecordInput recordInput = input.getInput(context);
            final RecordOutput recordOutput = output.getOutput(context);

            final ScalarInputField[] scalarInputFields = new ScalarInputField[input.getType(context).size()];
            final ScalarOutputField[] scalarOutputFields = new ScalarOutputField[scalarInputFields.length];

            for (int i = 0; i < scalarInputFields.length; i++)
            {
                scalarInputFields[i] = recordInput.getField(i);
                scalarOutputFields[i] = recordOutput.getField(i);
            }

            final DoubleInputField lonField = (DoubleInputField) recordInput.getField(m_lonField);
            final DoubleInputField latField = (DoubleInputField) recordInput.getField(m_latField);

            final StringOutputField attrField = (StringOutputField) recordOutput.getField(m_attributeField);

            final Point point = new Point();
            while (recordInput.stepNext())
            {
                point.setXY(lonField.asDouble(), latField.asDouble());

                for (final Feature feature : (ArrayList<Feature>) featureList)
                {
                    if (GeometryEngine.contains(feature.getMapGeometry().getGeometry(), point, feature.getMapGeometry().getSpatialReference()))
                    {
                        for (int i = 0; i < scalarInputFields.length; i++)
                        {
                            scalarOutputFields[i].set(scalarInputFields[i]);
                        }
                        attrField.set(feature.getAttribute());
                        recordOutput.push();
                        break;
                    }
                }
            }
            recordOutput.pushEndOfData();
        }
    }

    private final RecordPort m_input = newRecordInput("input");
    private final RecordPort m_lookup = newRecordInput("lookup");
    private final RecordPort m_output = newRecordOutput("output");

    private String m_lonField = "LON";
    private String m_latField = "LAT";

    private String m_geometryField = "SHAPE";
    private String m_attributeField = "ATTRIBUTE";

    @JsonCreator
    public LookupContainsOperator()
    {
    }

    public String getGeometryField()
    {
        return m_geometryField;
    }

    public void setGeometryField(final String geometryField)
    {
        m_geometryField = geometryField;
    }

    public String getAttributeField()
    {
        return m_attributeField;
    }

    public void setAttributeField(final String attributeField)
    {
        m_attributeField = attributeField;
    }

    public String getLonField()
    {
        return m_lonField;
    }

    public void setLonField(final String lonField)
    {
        m_lonField = lonField;
    }

    public String getLatField()
    {
        return m_latField;
    }

    public void setLatField(final String latField)
    {
        m_latField = latField;
    }

    @Override
    public RecordPort getInput()
    {
        return m_input;
    }

    @Override
    public RecordPort getOutput()
    {
        return m_output;
    }

    public RecordPort getLookup()
    {
        return m_lookup;
    }

    @Override
    protected void compose(final CompositionContext context)
    {
        final Builder builder = context.add(new Builder());
        context.connect(m_lookup, builder.input);

        final Walker walker = context.add(new Walker());
        context.connect(builder.model, walker.model);
        context.connect(m_input, walker.input);
        context.connect(walker.output, m_output);
    }

}

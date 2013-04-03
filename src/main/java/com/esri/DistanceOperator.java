package com.esri;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.pervasive.datarush.operators.AbstractExecutableRecordPipeline;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.ParallelismStrategy;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.ports.physical.DoubleInputField;
import com.pervasive.datarush.ports.physical.DoubleOutputField;
import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.physical.RecordOutput;
import com.pervasive.datarush.ports.physical.ScalarInputField;
import com.pervasive.datarush.ports.physical.ScalarOutputField;
import com.pervasive.datarush.types.Field;
import com.pervasive.datarush.types.RecordTokenTypeBuilder;
import com.pervasive.datarush.types.TokenTypeConstant;
import org.codehaus.jackson.annotate.JsonCreator;

public class DistanceOperator extends AbstractExecutableRecordPipeline
{
    private static final String METERS = "METERS";

    private String m_lonFieldName;

    private String m_latFieldName;

    private double m_lon;

    private double m_lat;

    private double m_minMeters = Double.NEGATIVE_INFINITY;

    private double m_maxMeters = Double.POSITIVE_INFINITY;

    @JsonCreator
    public DistanceOperator()
    {
    }

    public double getMinMeters()
    {
        return m_minMeters;
    }

    public void setMinMeters(final double minMeters)
    {
        m_minMeters = minMeters;
    }

    public double getMaxMeters()
    {
        return m_maxMeters;
    }

    public void setMaxMeters(final double maxMeters)
    {
        m_maxMeters = maxMeters;
    }

    public double getLat()
    {
        return m_lat;
    }

    public void setLat(final double lat)
    {
        m_lat = lat;
    }

    public double getLon()
    {
        return m_lon;
    }

    public void setLon(final double lon)
    {
        m_lon = lon;
    }

    public String getLonFieldName()
    {
        return m_lonFieldName;
    }

    public void setLonFieldName(final String lonFieldName)
    {
        m_lonFieldName = lonFieldName;
    }

    public String getLatFieldName()
    {
        return m_latFieldName;
    }

    public void setLatFieldName(final String latFieldName)
    {
        m_latFieldName = latFieldName;
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
        builder.addField(METERS, TokenTypeConstant.DOUBLE);

        output.setType(context, builder.toType());

        output.setOutputDataOrdering(context,
                input.getSourceDataOrdering(context));

        output.setOutputDataDistribution(context,
                input.getSourceDataDistribution(context));

    }

    @Override
    protected void execute(final ExecutionContext context)
    {
        final RecordInput recordInput = input.getInput(context);
        final RecordOutput recordOutput = output.getOutput(context);

        final ScalarInputField[] scalarInputFields = new ScalarInputField[input.getType(context).size()];
        final ScalarOutputField[] scalarOutputFields = new ScalarOutputField[scalarInputFields.length];

        for (int i = 0; i < scalarInputFields.length; i++)
        {
            scalarInputFields[i] = recordInput.getField(i);
            scalarOutputFields[i] = recordOutput.getField(i);
        }

        final DoubleInputField lonField = (DoubleInputField) recordInput.getField(m_lonFieldName);
        final DoubleInputField latField = (DoubleInputField) recordInput.getField(m_latFieldName);

        final DoubleOutputField metersField = (DoubleOutputField) recordOutput.getField(METERS);

        final Point destPoint = new Point(m_lon, m_lat);
        final Point origPoint = new Point();

        while (recordInput.stepNext())
        {
            origPoint.setXY(lonField.asDouble(), latField.asDouble());
            final double meters = GeometryEngine.geodesicDistanceOnWGS84(origPoint, destPoint);
            if (m_minMeters < meters && meters <= m_maxMeters)
            {
                for (int i = 0; i < scalarInputFields.length; i++)
                {
                    scalarOutputFields[i].set(scalarInputFields[i]);
                }
                metersField.set(meters);
                recordOutput.push();
            }
        }
        recordOutput.pushEndOfData();
    }

}

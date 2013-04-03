package com.esri;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.SpatialReference;
import com.pervasive.datarush.operators.AbstractExecutableRecordPipeline;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.ParallelismStrategy;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.ports.physical.DoubleInputField;
import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.physical.RecordOutput;
import com.pervasive.datarush.ports.physical.ScalarInputField;
import com.pervasive.datarush.ports.physical.ScalarOutputField;
import org.codehaus.jackson.annotate.JsonCreator;

public class ContainsOperator extends AbstractExecutableRecordPipeline
{
    // final private Log m_log = LogFactory.getLog(ContainsOperator.class);

    private String m_lonFieldName;

    private String m_latFieldName;

    private EsriPolygon m_polygon;

    @JsonCreator
    public ContainsOperator()
    {
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

    public EsriPolygon getPolygon()
    {
        return m_polygon;
    }

    public void setPolygon(final EsriPolygon polygon)
    {
        m_polygon = polygon;
    }

    @Override
    protected void computeMetadata(final StreamingMetadataContext context)
    {
        context.parallelize(ParallelismStrategy.NEGOTIATE_BASED_ON_SOURCE);

        output.setType(context, input.getType(context));

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

        final Point point = new Point();
        final SpatialReference spatialReference = toSpatialReference(m_polygon);
        final Polygon polygon = toPolygon(m_polygon);
        while (recordInput.stepNext())
        {
            point.setXY(lonField.asDouble(), latField.asDouble());
            if (GeometryEngine.contains(polygon, point, spatialReference))
            {
                for (int i = 0; i < scalarInputFields.length; i++)
                {
                    scalarOutputFields[i].set(scalarInputFields[i]);
                }
                recordOutput.push();
            }
        }
        recordOutput.pushEndOfData();
    }

    private Polygon toPolygon(final EsriPolygon esriPolygon)
    {
        final Polygon polygon = new Polygon();
        for (final EsriRing ring : esriPolygon.getRings())
        {
            final EsriCoords[] coords = ring.getCoords();
            polygon.startPath(coords[0].getX(), coords[0].getY());
            for (int i = 1; i < coords.length; i++)
            {
                polygon.lineTo(coords[i].getX(), coords[i].getY());
            }
            polygon.closePathWithLine();
        }
        return polygon;
    }

    private SpatialReference toSpatialReference(final EsriPolygon polygon)
    {
        return SpatialReference.create(polygon.getSpatialReference().getWkid());
    }

}

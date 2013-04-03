package com.esri;

import com.pervasive.datarush.operators.AbstractExecutableRecordPipeline;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.ParallelismStrategy;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.ports.physical.DoubleInputField;
import com.pervasive.datarush.ports.physical.DoubleOutputField;
import com.pervasive.datarush.ports.physical.LongOutputField;
import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.physical.RecordOutput;
import com.pervasive.datarush.types.RecordTokenTypeBuilder;
import com.pervasive.datarush.types.TokenTypeConstant;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 */
public class BinOperator extends AbstractExecutableRecordPipeline
{
    private Log m_log = LogFactory.getLog(BinOperator.class);

    private String m_keyFieldName = "ROWCOL";
    private String m_valFieldName = "POPULATION";
    private String m_lonFieldName = "LON";
    private String m_latFieldName = "LAT";

    private double m_minLon = -180.0;
    private double m_maxLon = 180.0;
    private double m_minLat = -90.0;
    private double m_maxLat = 90.0;
    private double m_binSize = 1.0;

    @JsonCreator
    public BinOperator()
    {
    }

    public double getMaxLat()
    {
        return m_maxLat;
    }

    public void setMaxLat(final double maxLat)
    {
        m_maxLat = maxLat;
    }

    public double getMaxLon()
    {
        return m_maxLon;
    }

    public void setMaxLon(final double maxLon)
    {
        m_maxLon = maxLon;
    }

    public double getBinSize()
    {
        return m_binSize;
    }

    public void setBinSize(final double binSize)
    {
        m_binSize = binSize;
    }

    public double getMinLat()
    {
        return m_minLat;
    }

    public void setMinLat(final double minLat)
    {
        m_minLat = minLat;
    }

    public double getMinLon()
    {
        return m_minLon;
    }

    public void setMinLon(final double minLon)
    {
        m_minLon = minLon;
    }

    public String getLatFieldName()
    {
        return m_latFieldName;
    }

    public void setLatFieldName(final String latFieldName)
    {
        m_latFieldName = latFieldName;
    }

    public String getLonFieldName()
    {
        return m_lonFieldName;
    }

    public void setLonFieldName(final String lonFieldName)
    {
        m_lonFieldName = lonFieldName;
    }

    public String getKeyFieldName()
    {
        return m_keyFieldName;
    }

    public void setKeyFieldName(final String keyFieldName)
    {
        m_keyFieldName = keyFieldName;
    }

    public String getValFieldName()
    {
        return m_valFieldName;
    }

    public void setValFieldName(final String valFieldName)
    {
        m_valFieldName = valFieldName;
    }

    @Override
    protected void computeMetadata(final StreamingMetadataContext context)
    {
        context.parallelize(ParallelismStrategy.NEGOTIATE_BASED_ON_SOURCE);

        final RecordTokenTypeBuilder builder = new RecordTokenTypeBuilder();

        builder.addField(m_keyFieldName, TokenTypeConstant.LONG);
        builder.addField(m_valFieldName, TokenTypeConstant.DOUBLE);

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

        final DoubleInputField lonField = (DoubleInputField) recordInput.getField(m_lonFieldName);
        final DoubleInputField latField = (DoubleInputField) recordInput.getField(m_latFieldName);

        final LongOutputField keyField = (LongOutputField) recordOutput.getField(m_keyFieldName);
        final DoubleOutputField valField = (DoubleOutputField) recordOutput.getField(m_valFieldName);

        while (recordInput.stepNext())
        {
            final double x = lonField.asDouble();
            final double y = latField.asDouble();

            if (x < m_minLon || x > m_maxLon || y < m_minLat || y > m_maxLat)
            {
                continue;
            }

            final long col = (long) Math.floor((x - m_minLon) / m_binSize) & 0x7FFFL;
            final long row = (long) Math.floor((y - m_minLat) / m_binSize) & 0x7FFFL;

            keyField.set((row << 32) | col);
            valField.set(1.0);

            recordOutput.push();
        }
        recordOutput.pushEndOfData();

    }
}

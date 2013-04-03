package com.esri;

import com.pervasive.datarush.operators.ExecutableOperator;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.ParallelismStrategy;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.record.RecordPort;
import com.pervasive.datarush.tokens.scalar.DoubleValued;
import com.pervasive.datarush.tokens.scalar.LongValued;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonCreator;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class RasterOperator extends ExecutableOperator
{
    private final Log m_log = LogFactory.getLog(RasterOperator.class);

    private final RecordPort input = newRecordInput("input");

    private String m_keyFieldName = "ROWCOL";
    private String m_valFieldName = "POPULATION";
    private String m_path = "/tmp/raster.asc";

    private double m_minx = -180.0;
    private double m_miny = -90.0;
    private double m_maxx = 180.0;
    private double m_maxy = 90;
    private double m_cellSize = 1.0;

    @JsonCreator
    public RasterOperator()
    {
    }

    public RasterOperator(final String path)
    {
        m_path = path;
    }

    public RecordPort getInput()
    {
        return input;
    }

    public String getPath()
    {
        return m_path;
    }

    public void setPath(final String path)
    {
        m_path = path;
    }

    public double getCellSize()
    {
        return m_cellSize;
    }

    public void setCellSize(final double cellSize)
    {
        m_cellSize = cellSize;
    }

    public double getMaxx()
    {
        return m_maxx;
    }

    public void setMaxx(final double maxx)
    {
        m_maxx = maxx;
    }

    public double getMaxy()
    {
        return m_maxy;
    }

    public void setMaxy(final double maxy)
    {
        m_maxy = maxy;
    }

    public double getMinx()
    {
        return m_minx;
    }

    public void setMinx(final double minx)
    {
        m_minx = minx;
    }

    public double getMiny()
    {
        return m_miny;
    }

    public void setMiny(final double miny)
    {
        m_miny = miny;
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
        context.parallelize(ParallelismStrategy.NON_PARALLELIZABLE);
        context.setClientExecution(true);
    }

    @Override
    protected void execute(final ExecutionContext context)
    {
        final RecordInput input = this.input.getInput(context);

        final LongValued keyField = (LongValued) input.getField(m_keyFieldName);
        final DoubleValued valField = (DoubleValued) input.getField(m_valFieldName);

        final Map<Long, Double> map = new HashMap<Long, Double>();

        while (input.stepNext())
        {
            final long key = keyField.asLong();
            final double val = valField.asDouble();
            final long row = key >> 32;
            final long col = key & 0x7FFFL;
            map.put(key, val);
            if (m_log.isDebugEnabled())
            {
                m_log.debug(String.format("%d %d %f", row, col, val));
            }
        }

        final int ncols = (int) Math.floor((m_maxx - m_minx) / m_cellSize);
        final int nrows = (int) Math.floor((m_maxy - m_miny) / m_cellSize);

        try
        {
            if (m_path.indexOf(".asc") != -1)
            {
                writeAscii(map, nrows, ncols, m_minx, m_miny);
            }
            else
            {
                writeHeader(m_path, nrows, ncols, m_minx, m_miny);
                writeFloat(m_path, nrows, ncols, map);
            }
        }
        catch (IOException e)
        {
            if (m_log.isErrorEnabled())
            {
                m_log.error(e.toString());
            }
        }
    }

    private void writeFloat(
            final String filename,
            final int nrows,
            final int ncols,
            final Map<Long, Double> map) throws IOException
    {
        final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
        try
        {
            long i = nrows - 1;
            for (int r = 0; r < nrows; r++)
            {
                final long row = i << 32;
                for (int c = 0; c < ncols; c++)
                {
                    final Double val = map.get(row | c);
                    dos.writeFloat(val != null ? val.floatValue() : 0.0f);
                }
                i--;
            }
        }
        finally
        {
            dos.close();
        }
    }

    private void writeHeader(
            final String filename,
            final int nrows,
            final int ncols,
            final double minx,
            final double miny) throws FileNotFoundException
    {
        final PrintWriter pw = new PrintWriter(new File(filename.replaceFirst(".flt", ".hdr")));
        try
        {
            pw.print("NCOLS ");
            pw.println(ncols);
            pw.print("NROWS ");
            pw.println(nrows);
            pw.print("XLLCORNER ");
            pw.println(minx);
            pw.print("YLLCORNER ");
            pw.println(miny);
            pw.print("CELLSIZE ");
            pw.println(m_cellSize);
            pw.println("NODATA_VALUE 0");
            pw.println("BYTEORDER MSBFIRST");
        }
        finally
        {
            pw.close();
        }
    }

    private void writeAscii(
            final Map<Long, Double> map,
            final int nrows,
            final int ncols,
            final double minx,
            final double miny) throws FileNotFoundException
    {
        final PrintWriter pw = new PrintWriter(new BufferedOutputStream(new FileOutputStream(m_path), 10 * 1024));
        try
        {
            pw.print("NCOLS ");
            pw.println(ncols);
            pw.print("NROWS ");
            pw.println(nrows);
            pw.print("XLLCORNER ");
            pw.println(minx);
            pw.print("YLLCORNER ");
            pw.println(miny);
            pw.print("CELLSIZE ");
            pw.println(m_cellSize);
            pw.println("NODATA_VALUE 0");
            long i = nrows - 1;
            for (int r = 0; r < nrows; r++)
            {
                final long row = i << 32;
                for (int c = 0; c < ncols; c++)
                {
                    final Double val = map.get(row | c);
                    if (val != null)
                    {
                        pw.print(val.doubleValue());
                        pw.print(' ');
                    }
                    else
                    {
                        pw.print("0 ");
                    }
                }
                i--;
                pw.println();
            }

        }
        finally
        {
            pw.close();
        }
    }


}

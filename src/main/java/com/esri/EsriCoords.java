package com.esri;

import org.codehaus.jackson.annotate.JsonCreator;

import java.io.Serializable;

/**
 */
public class EsriCoords implements Serializable
{
    private double m_x;

    private double m_y;

    @JsonCreator
    public EsriCoords()
    {
    }

    public EsriCoords(
            final double x,
            final double y)
    {
        this.m_x = x;
        this.m_y = y;
    }

    public double getX()
    {
        return m_x;
    }

    public void setX(final double x)
    {
        m_x = x;
    }

    public double getY()
    {
        return m_y;
    }

    public void setY(final double y)
    {
        m_y = y;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final EsriCoords coords = (EsriCoords) o;

        if (Double.compare(coords.m_x, m_x) != 0)
        {
            return false;
        }
        if (Double.compare(coords.m_y, m_y) != 0)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result;
        long temp;
        temp = m_x != +0.0d ? Double.doubleToLongBits(m_x) : 0L;
        result = (int) (temp ^ (temp >>> 32));
        temp = m_y != +0.0d ? Double.doubleToLongBits(m_y) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}

package com.esri;

import org.codehaus.jackson.annotate.JsonCreator;

import java.io.Serializable;

/**
 */
public class EsriSpatialReference implements Serializable
{
    private int m_wkid;

    @JsonCreator
    public EsriSpatialReference()
    {
    }

    public EsriSpatialReference(final int wkid)
    {
        this.m_wkid = wkid;
    }

    public int getWkid()
    {
        return m_wkid;
    }

    public void setWkid(final int wkid)
    {
        m_wkid = wkid;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof EsriSpatialReference))
        {
            return false;
        }

        final EsriSpatialReference that = (EsriSpatialReference) o;

        if (m_wkid != that.m_wkid)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return m_wkid;
    }
}

package com.esri;

import org.codehaus.jackson.annotate.JsonCreator;

import java.io.Serializable;

/**
 */
public class EsriRing implements Serializable
{
    private EsriCoords[] m_coords;

    @JsonCreator
    public EsriRing()
    {
    }

    public EsriRing(final EsriCoords[] coords)
    {
        m_coords = coords;
    }

    public EsriCoords[] getCoords()
    {
        return m_coords;
    }

    public void setCoords(final EsriCoords[] coords)
    {
        m_coords = coords;
    }
}

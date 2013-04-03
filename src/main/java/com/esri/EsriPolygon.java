package com.esri;

import org.codehaus.jackson.annotate.JsonCreator;

/**
 */
public class EsriPolygon extends EsriGeometry
{
    private EsriRing[] m_rings;

    @JsonCreator
    public EsriPolygon()
    {
    }

    public EsriPolygon(final EsriSpatialReference spatialReference)
    {
        super(spatialReference);
    }

    public EsriPolygon(
            final EsriSpatialReference spatialReference,
            final EsriRing[] rings)
    {
        super(spatialReference);
        m_rings = rings;
    }

    public EsriRing[] getRings()
    {
        return m_rings;
    }

    public void setRings(final EsriRing[] rings)
    {
        this.m_rings = rings;
    }

}

package com.esri;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;

/**
 */
public class EsriPoint extends EsriGeometry
{
    private EsriCoords m_coords;

    @JsonCreator
    public EsriPoint()
    {
    }

    public EsriPoint(final EsriSpatialReference spatialReference)
    {
        super(spatialReference);
    }

    public EsriPoint(
            final EsriSpatialReference spatialReference,
            final EsriCoords coords)
    {
        super(spatialReference);
        m_coords = coords;
    }

    public EsriCoords getCoords()
    {
        return m_coords;
    }

    public void setCoords(final EsriCoords coords)
    {
        m_coords = coords;
    }

    @JsonIgnore
    public double getX()
    {
        return m_coords.getX();
    }

    public void setX(double x)
    {
        m_coords.setX(x);
    }

    @JsonIgnore
    public double getY()
    {
        return m_coords.getY();
    }

    public void setY(double y)
    {
        m_coords.setY(y);
    }
}

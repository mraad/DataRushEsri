package com.esri;

import org.codehaus.jackson.annotate.JsonCreator;

/**
 */
public class EsriGeometry implements IGeometry
{
    protected EsriSpatialReference m_spatialReference;

    @JsonCreator
    public EsriGeometry()
    {
    }

    public EsriGeometry(final EsriSpatialReference spatialReference)
    {
        m_spatialReference = spatialReference;
    }

    @Override
    public EsriSpatialReference getSpatialReference()
    {
        return m_spatialReference;
    }

    public void setSpatialReference(final EsriSpatialReference spatialReference)
    {
        m_spatialReference = spatialReference;
    }
}

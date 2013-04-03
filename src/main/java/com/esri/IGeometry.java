package com.esri;

import java.io.Serializable;

/**
 */
public interface IGeometry extends Serializable
{
    public EsriSpatialReference getSpatialReference();
}

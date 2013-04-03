DataRushEsri
============

[DataRush Spatial Operators](http://thunderheadxpler.blogspot.com/2013/04/bigdata-datarush-workflow-in-arcmap.html)

# Prerequisites

This [maven](http://maven.apache.org) project depends on [Pervasive DataRush SDK](http://bigdata.pervasive.com) and on the GIS Tools for Hadoop [Esri geometry API](https://github.com/Esri/geometry-api-java "Esri geometry API").
You need to clone the geometry project, compile it and copy the jar into you own local maven repo.

    $ cd <your-work-folder>
    $ git clone https://github.com/Esri/geometry-api-java.git
    $ cd geometry-api-java
    $ ant
    $ mvn install:install-file -Dfile=esri-geometry-api.jar -DgroupId=com.esri -DartifactId=esri-geometry-api -Dversion=1.0 -Dpackaging=jar -DgeneratePom=true

# Compiling and install the jar

    $ mvn -P cdh4 clean install

# Generate local sample data

    $ awk -f points.awk > /tmp/points.txt
    $ awk -f poolygons.awk > /tmp/polygons.txt

# Put sample data in HDFS

    $ hadoop fs -mkdir points
    $ awk -f points.awk | hadoop fs -put - points/points.txt
    $ hadoop fs -mkdir polygons
    $ awk -f polygons.awk | hadoop fs -put - polygons/polygons.txt

# Using CDH4 profile application

    $ mvn -P cdh4 clean package appassembler:assemble
    $ sh target/appassembler/bin/App cdh4

DataRushEsri
============

DataRush Spatial Operators

# Compiling and install the jar

    $ mvn -P cdh4 clean install

# Using CDH4 profile application

    $ mvn -P cdh4 clean package appassembler:assemble
    $ sh target/appassembler/bin/App cdh4

# Generate local sample data

    $ awk -f points.awk > /tmp/points.txt
    $ awk -f poolygons.awk > /tmp/polygons.txt

# Put sample data in HDFS

    $ hadoop fs -mkdir points
    $ awk -f points.awk | hadoop fs -put - points/points.txt
    $ hadoop fs -mkdir polygons
    $ awk -f polygons.awk | hadoop fs -put - polygons/polygons.txt

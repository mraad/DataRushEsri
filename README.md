DataRushEsri
============

DataRush Spatial Operators

# Compiling the jar

    $ mvn -P cdh4 clean install

# Using CDH4 profile application

    $ mvn -P cdh4 clean package appassembler:assemble
    $ sh target/appassembler/bin/App cdh4

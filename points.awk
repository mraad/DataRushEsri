BEGIN{
    srand()
    OFS="\t"
    print "LON","LAT"
    for(I=0;I<1000;I++){
        LON=-180+360*rand()
        LAT=-90+180*rand()
        print LON,LAT
    }
}

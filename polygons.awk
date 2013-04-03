BEGIN{
    srand()
    OFS="\t"
     for(I=0;I<100;I++){
         X=-180+360*rand()
         Y=-90+180*rand()
         W=1+19*rand()
         H=1+19*rand()

         P0="[" X "," Y "]"
         P1="[" X+W "," Y "]"
         P2="[" X+W "," Y+H "]"
         P3="[" X "," Y+H "]"

         PATH="[" P0 "," P1 "," P2 "," P3 "," P0 "]"

         RINGS="[" PATH "]"
         JSON = "{\"rings\":" RINGS "}"
         print JSON,"POLY"I
     }
 }
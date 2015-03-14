#! /bin/bash
# @author manish
# small script to take care of packaging using sbt and run 
# have kept all necessary path set in bashrc
sbt package

if [ $? -eq 0 ]; then
    echo OK
else
    echo FAIL
    exit 0
fi
# All cores => I have tested on 2 
spark-submit --class ImageSVD --master "local[*]" target/scala-2.11/manishproject_2.11-1.0.jar $@   
# one core 
#spark-submit --class ImageSVD --master "local" target/scala-2.11/manishproject_2.11-1.0.jar $@   

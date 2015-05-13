#!/bin/bash

echo "Good morning, world."


INPUTDIR=/hadoop_naresh/data/input/crystalBall/
OUTPUTDIR=/hadoop_naresh/data/output/crystalBall
APPNAME=CrystalBall
JARFILE=/home/hadoop_naresh/hadoop/applicationJar/crystalBall.jar
ALGONAME=pair
REDUCERNO=1
echo
echo
echo "----------------------------------CRYSTAL BALL-----------------------------------------"
echo "---------------------------------------------------------------------------------------"
echo "default alogrithm : $ALGONAME"
echo "default input path: $INPUTDIR"
echo "default output path : $OUTPUTDIR"
echo "default reducer no.: $REDUCERNO"
echo "---------------------------------------------------------------------------------------"
echo 
echo
echo



echo "Input Jar path :"

read jarPath

if [ -n "$jarPath" ]
then
echo "jar path : $jarPath"
JARFILE=$jarPath
fi

echo $JARFILE
echo

echo "Input Algorithm :"

read algoName

if [ -n "$algoName" ]
then
ALGONAME=$algoName
fi

echo $ALGONAME
echo


echo "Input input path:"
read inputPath

if [ -n "$inputPath" ]
then
INPUTDIR=$inputPath
fi

echo $INPUTDIR
echo

echo "Input output path:"

read outputPath

if [ -n "$outputPath" ]
then
OUTPUTDIR=$outputPath
fi

echo $OUTPUTDIR
echo

echo "Number of reducers: "
read reduNo

if [ -n "$reduNo" ]
then
REDUCERNO=$reduNo
fi

echo $REDUCERNO
echo 

echo "---------------------------------------------------------------------------"
echo "Executing statement"
echo "hadoop jar $JARFILE $APPNAME $INPUTDIR $OUTPUTDIR $ALGONAME $REDUCERNO"
echo "---------------------------------------------------------------------------"
echo "---------------------------------------------------------------------------"
echo "---------------------------------------------------------------------------"

hadoop jar $JARFILE $APPNAME $INPUTDIR $OUTPUTDIR $ALGONAME $REDUCERNO

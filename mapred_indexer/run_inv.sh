#!/bin/bash

export JAVA_HOME=/usr

###### pipeline step one 
#rm -rf output1
#../bin/hadoop jar ./dist/InvertedIndexer.jar edu.umich.cse.eecs485.InvertedIndex1 ../dataset/test output1

###### pipeline step two 
#rm -rf output2
#../bin/hadoop jar ./dist/InvertedIndexer.jar edu.umich.cse.eecs485.InvertedIndex2 output1 output2

###### pipeline step three 
#rm -rf output3
#../bin/hadoop jar ./dist/InvertedIndexer.jar edu.umich.cse.eecs485.InvertedIndex3 output2 output3

###### pipeline step three 
rm -rf output4
../bin/hadoop jar ./dist/InvertedIndexer.jar edu.umich.cse.eecs485.InvertedIndex4 output3 output4




#!/bin/bash

export JAVA_HOME=/usr

# pipeline step one 
rm -rf output1
../bin/hadoop jar ./dist/InvertedIndexer.jar edu.umich.cse.eecs485.InvertedIndex1 ../dataset/test output1

# pipeline step two 
rm -rf output2
../bin/hadoop jar ./dist/InvertedIndexer.jar edu.umich.cse.eecs485.InvertedIndex2 output1 output2




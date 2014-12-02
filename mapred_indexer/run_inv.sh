#!/bin/bash

export JAVA_HOME=/usr

# pipeline step one 
rm -rf output1
../bin/hadoop jar ./dist/InvertedIndex1.jar edu.umich.cse.eecs485.InvertedIndex1 ../dataset/test output1

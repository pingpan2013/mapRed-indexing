all: InvertedIndex1 InvertedIndex2 InvertedIndex3

InvertedIndex1:
	javac -cp .:../hadoop-core-1.0.1.jar:../lib/* -d bin ./src/InvertedIndex1.java ./src/Xmlinputformat.java 
	jar -cvf ./dist/InvertedIndexer.jar -C bin/ .

InvertedIndex2:
	javac -cp .:../hadoop-core-1.0.1.jar:../lib/* -d bin ./src/InvertedIndex2.java
	jar -cvf ./dist/InvertedIndexer.jar -C bin/ .

InvertedIndex3:
	javac -cp .:../hadoop-core-1.0.1.jar:../lib/* -d bin ./src/InvertedIndex3.java
	jar -cvf ./dist/InvertedIndexer.jar -C bin/ .

clean:
	rm -rf ./dist/*.jar bin/* output*

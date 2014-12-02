all: InvertedIndex1

InvertedIndex1:
	javac -cp .:../hadoop-core-1.0.1.jar:../lib/* -d bin ./src/InvertedIndex1.java ./src/Xmlinputformat.java 
	jar -cvf ./dist/InvertedIndex1.jar -C bin/ .

clean:
	rm -rf ./dist/*.jar bin/* output*

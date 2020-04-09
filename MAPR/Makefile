startDataNodeServer: all.class
	# start rmi registry in the root directory of class files
	java -classpath "./bin:./src/com/google/protobuf/protobuf-java-3.11.4.jar" -Djava.rmi.server.codebase=file:bin/ ds.hdfs.DataNode 

all.class:
	javac -d bin -classpath "./:./src/com/google/protobuf/protobuf-java-3.11.4.jar" src/ds/hdfs/Client.java src/ds/hdfs/DataNode.java src/ds/hdfs/NameNode.java src/ds/hdfs/marshallstuff.java src/ds/hdfs/NetworkTest.java src/ds/hdfs/IDataNode.java src/ds/hdfs/INameNode.java src/ds/hdfs/temporaryInterface.java






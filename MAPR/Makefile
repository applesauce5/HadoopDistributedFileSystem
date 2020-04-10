startDataNodeServerLS: all.class
	java -classpath "./bin:./src/com/google/protobuf/protobuf-java-3.11.4.jar" -Djava.rmi.server.codebase=file:bin/ ds.hdfs.DataNode src/dn_config.txt ls src/nn_config.txt

startDataNodeServerLESS: all.class
	java -classpath "./bin:./src/com/google/protobuf/protobuf-java-3.11.4.jar" -Djava.rmi.server.codebase=file:bin/ ds.hdfs.DataNode src/dn_config.txt less src/nn_config.txt

startNameNodeServer: all.class
	java -classpath "./bin:./src/com/google/protobuf/protobuf-java-3.11.4.jar" -Djava.rmi.server.codebase=file:bin/ ds.hdfs.NameNode src/nn_config.txt

startClient: all.class
	java -classpath "./bin:./src/com/google/protobuf/protobuf-java-3.11.4.jar" -Djava.rmi.server.codebase=file:bin/ ds.jdfs.Client src/configurations.txt src/nn_config.txt

all.class:
	javac -d bin -classpath "./:./src/com/google/protobuf/protobuf-java-3.11.4.jar" src/ds/hdfs/Client.java src/ds/hdfs/DataNode.java src/ds/hdfs/NameNode.java src/ds/hdfs/marshallstuff.java src/ds/hdfs/NetworkTest.java src/ds/hdfs/IDataNode.java src/ds/hdfs/INameNode.java src/ds/hdfs/temporaryInterface.java






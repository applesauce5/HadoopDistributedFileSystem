
package ds.hdfs;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
//import com.testCode.Hello;
//import com.testCode.ImplementHello;

import java.io.*;
import java.nio.charset.Charset;

//import ds.hdfs.hdfsformat.*;
import ds.hdfs.IDataNode.*;

import ds.hdfs.marshallstuff.*;

/**
 * >>> Should be used for performing read and write operations of blocks. <<<
 *
 * -> Design the message protocol using protobuf. That will help you in standardizing your work across functions and files.
 * -> You should use a configuration file to help change different run-time parameters without recompiling the source.
 * -> Block size: 64Bytes (should be configurable in your configuration file) [Mention its name it in your README]
 * -> Block replication Factor: Two way.
 * -> NN should print a warning message if some DN(s) are down (no heartbeats) and some file is not readable due to it.
 * --->>> DNs should also support restart and all operations should be carried on ordinarily after NN server is up and running again.
 * --->>> HDFS should persist its state even in the event of NN restart
 * @author mcho5
 *
 */
public class DataNode implements IDataNode
{
    protected String MyChunksFile;
    protected INameNode NNStub;
    protected String MyIP;
    protected int MyPort;
    protected String MyName;
    protected int MyID;

    public DataNode(String ip, int port, String name) {
        //Constructor
    	// perhaps do something here
    	this.MyIP = ip;
    	this.MyPort = port;
    	this.MyName = name;
    }

    // gather corresponding blocks and persist into file??
    // ignore ---------------------------------------------Project 2
    /**
    public static void appendtoFile(String Filename, String Line)
    {
        BufferedWriter bw = null;

        try {
        	//implement here
            //append
        }
        catch (IOException ioe)
        {
            ioe.printStackTrace();
        }
        finally
        {
        	// always close the file
            if (bw != null) try {
                bw.close();
            } catch (IOException ioe2) {
            }
        }

    }
    **/
    /**
     * --> Create and return a unique handle for each opened file.
     * Get all block locations for the file
     * Read blocks in sequence Workflow:
     * 	openFile("filename")
     * 		in a loop:
     * 			getBlockLocations() using handle from openFile
     * 			Obtain a reference to the remote DN object using an entry in the DataNode location.
     * 			Issue readBlock() to the DN (if this fails, try the next DN)
     * 			Write to the local file.
     * 	closeFile()
     */
    public synchronized byte[] readBlock(byte[] Inp) {

    	chunkInfo.Builder newRes = chunkInfo.newBuilder();
    	try {
    		chunkInfo input = chunkInfo.parseFrom(Inp);
        	String filename = input.getFilename();

        	File file = new File(filename);
        	byte[] bytesArray = new byte[(int) file.length()];
    	    FileInputStream fis = new FileInputStream(file);
    	    fis.read(bytesArray); //read file into bytes[]
    	    fis.close();

        	ByteString bytstrfile = ByteString.copyFrom(bytesArray);

        	newRes.setFilename(filename);
        	newRes.setFileData(bytstrfile);

        }
        catch(Exception e)
        {
            System.out.println("Error at readBlock");
            //newRes.setStatus(-1);
        }

        return newRes.build().toByteArray();
    }

   /**
    * --> Create and return a unique handle for each opened file.
    * Write contents to the assigned block number
    * Workflow:
    * 	openFile("filename", 'r'): (throw error if the filename already exists)
    * 		in a loop:
    * 			1. Call assignBlock() using handle from openFile
    * 			2. Obtain a reference to the remote DataNode object using the first entry in the DataNode location
	* 			3. Call writeBlock() on all the assigned DataNodes
	* 	closeFile()
    */
    public synchronized byte[] writeBlock(byte[] Inp) {
    	chunkInfo.Builder newRes = chunkInfo.newBuilder();
        try {
        	// implement
        	chunkInfo input = chunkInfo.parseFrom(Inp);
        	String filename = input.getFilename();

        	File file = new File(filename);
        	ByteString store = input.getFileData();

        	store.writeTo(new FileOutputStream(file)); // block is written into the file

        }
        catch(Exception e)
        {
            System.out.println("Error at writeBlock ");
            //response.setStatus(-1);
        }

        return newRes.build().toByteArray();
    }

    // BlockReports sent with HeartBeats to the NameNode
    // NameNode then knows the locations of each block and tracks this information in memory
    public synchronized void BlockReport() throws IOException {
    	// call NameNode's Block report and heartbeat messages
    	 INameNode tmpNameNode = GetNNStub("NameNode","cp.cs.rutgers.edu",1099);
    	 this.NNStub = tmpNameNode;
    	 DataNodeInfo.Builder response = DataNodeInfo.newBuilder();
    	 response.setIp(this.MyIP);
    	 response.setPort(this.MyPort);
    	 response.setServerName(this.MyName);

    	 byte[] DataNodebyte = response.build().toByteArray();
    	 tmpNameNode.heartBeat(DataNodebyte);
    	 tmpNameNode.blockReport(DataNodebyte);
    	 System.out.println("Block Report Sent");
    }

    // Socket programming
    // Server code
    public void BindServer(String Name, String IP, int Port) {
        try
        {
            IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(this, 0);
            System.setProperty("java.rmi.server.hostname", IP);
            Registry registry = LocateRegistry.getRegistry(Port);
            registry.rebind(Name, stub);
            System.out.println("\nDataNode connected to RMIregistry\n");
        }catch(Exception e){
            System.err.println("Server Exception: " + e.toString());
            e.printStackTrace();
        }
    }

    // Server code stuff
    // DataNode is supposed to send heart beats to namenode + block report
    // Seen in client code <<<<<------------------------
    public INameNode GetNNStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try
            {
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                INameNode stub = (INameNode) registry.lookup(Name);
                System.out.println("NameNode Found!");
                return stub;
            }catch(Exception e){
                System.out.println("NameNode still not Found");
                continue;
            }
        }
    }

    /**
     * -> Clients and DN discover the NN from a conf file and
     * read from a standardized location (/tmp/somefile).
     *
     * -> DataNode needs to find corresponding NameNode once it has been assigned
     *
     * -> heartbeats are sent to the NameNode
     *
     * -> The conf file contains the socket information of the NN (port number).
     *
     * @param args
     * @throws InvalidProtocolBufferException
     * @throws IOException
     */
    public static void main(String args[]) throws InvalidProtocolBufferException, IOException, InterruptedException {
        // Define a Datanode Me
    
        DataNode Me = new DataNode("ls.cs.rutgers.edu",1099,"DataNode");

        /*
         * Server code
         */

        Me.BindServer(Me.MyName,Me.MyIP,Me.MyPort);
        // Sending block report every 3 seconds
        while(true) {
            System.err.println("Sending Block Report");
            Me.BlockReport();
            TimeUnit.SECONDS.sleep(3);
        }
        /**
        System.setProperty("java.rmi.server.hostname","ls.cs.rutgers.edu");

        try {
            IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(Me, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.bind("DataNode", stub);
            while(true) {
	            System.err.println("Server ready");
	            Me.BlockReport();
	            TimeUnit.SECONDS.sleep(3);
            }
        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
         **/
    }
}

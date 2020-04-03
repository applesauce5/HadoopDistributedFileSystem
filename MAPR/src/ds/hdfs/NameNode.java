 package ds.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.InvalidProtocolBufferException;
import com.testCode.Hello;
import com.testCode.ImplementHello;
import com.google.protobuf.*;

//import ds.hdfs.hdfsformat.*;

import ds.hdfs.marshallstuff.*;
/**
 * >>>  storing information about the files in the system and handling the communication with DataNodes. <<<
 * >>> NN should be able to perform all major file operations such as open, close, list and should support a 
 *     block management mechanism similar to HDFS <<<
 *     
 * -> NameNode is considered to be the master and is in charge of keeping information about the DataNodes and the address/name-space 
 * of the chunks across the chunk-servers/DataNodes
 * 
 *  //	1. Open the file to be written to HDFS
	//	2. Depending on the size, you know how many blocks you will need.
	//	3. For each block, ask the NN for a list of DNs where you will replicate them
	//	4. Write that block on each of these DNs
	//	5. Loop till the file is completely written.
	//	6. Close file
 * 
 * -> Design the message protocol using protobuf. That will help you in standardizing your work across functions and files. 
 * @author mcho5
 *
 */

public class NameNode implements INameNode{

	protected Registry serverRegistry;
	
	String ip;
	int port;
	String name;
	
	public NameNode(String addr,int p, String nn)
	{
		ip = addr;
		port = p;
		name = nn;
	}
	
	public static class DataNode 
	{
		String ip;
		int port;
		String serverName;
		public DataNode(String addr,int p,String sname)
		{
			ip = addr;
			port = p;
			serverName = sname;
		}
	}
	
	public static LinkedList<DataNode> dataNodeList;
	
	
	public static class FileInfo
	{
		String filename;
		int filehandle;
		boolean writemode;
		ArrayList<String> Chunks; // possibly address of the chunks???
		int replication;
		public FileInfo(String name, int handle, boolean option,int rep)
		{
			filename = name;
			filehandle = handle;
			writemode = option;
			Chunks = new ArrayList<String>();
			replication = rep;
		}
	}
	
	public static LinkedList<FileInfo> fileInfoList;
	
	/* Open a file given file name with read-write flag*/
	boolean findInFilelist(int fhandle) {
		// default until method is filled out
		return false;
	}
	
	// 2: list of files in HDFS
	public void printFilelist() {
		// default until method is filled out
	}
	
	// 1: The client creates a pipeline, after it has info in address info on the DataNode, to the DataNode and writes

	public synchronized byte[] openFile(byte[] inp) throws RemoteException {
		// maybe input stream output stream related
		ds.hdfs.marshallstuff.FileInfo.Builder response = ds.hdfs.marshallstuff.FileInfo.newBuilder(); 
		try{
			//implement
			ds.hdfs.marshallstuff.FileInfo Inp = ds.hdfs.marshallstuff.FileInfo.parseFrom(inp);
			File file = new File(Inp.getFilename());
			boolean exists = file.exists();
			
			if(file.exists() && file.isFile()) {
				System.out.println("File is a file and exists");
				response.setWritemode(true);
				response.setFilename(Inp.getFilename());
				response.setFilehandle(Inp.getFilehandle());
				response.setReplication(Inp.getReplication());
				for(String i : Inp.getChunkListList()) {
					response.addChunkList(i);
				}
			} else {
				System.out.println("File does not exist");
			}
		}
		catch (Exception e) 
		{
			System.err.println("Error at " + this.getClass() + e.toString());
			e.printStackTrace();
			//response.setStatus(-1);
		}
		// servlett response
		return response.build().toByteArray();
	}
	
	// 2: closes file after writing

	public synchronized byte[] closeFile(byte[] inp ) throws RemoteException {
		ds.hdfs.marshallstuff.FileInfo.Builder response = ds.hdfs.marshallstuff.FileInfo.newBuilder(); 
		try{
			//implement
		}
		catch(Exception e)
		{
			System.err.println("Error at closefileRequest " + e.toString());
			e.printStackTrace();
		//	response.setStatus(-1);
		}
		
		return response.build().toByteArray();
	}
	
	// given a block (or list of blocks) provide the corresponding location of this block

	public synchronized byte[] getBlockLocations(byte[] inp ) throws RemoteException {
		ds.hdfs.marshallstuff.FileInfo.Builder response = ds.hdfs.marshallstuff.FileInfo.newBuilder();
		try {
			//implement
			ds.hdfs.marshallstuff.FileInfo Inp = ds.hdfs.marshallstuff.FileInfo.parseFrom(inp);
			response.setWritemode(true);
			response.setFilename(Inp.getFilename());
			response.setFilehandle(Inp.getFilehandle());
			response.setReplication(Inp.getReplication());
			if(fileInfoList.size() == 0) {
				System.out.println("No files in the file system");
			} else {
				for(FileInfo i : fileInfoList) {
					if(i.filename.equals(Inp.getFilename())) {
						response.addAllChunkList(i.Chunks);
					}
				}
			}
		}
		catch(Exception e)
		{
			System.err.println("Error at getBlockLocations "+ e.toString());
			e.printStackTrace();
			//response.setStatus(-1);
		}		
		return response.build().toByteArray(); 
	}
	
	// you have a large file ----------> break file up into possibly a list of blocks
	public synchronized byte[] assignBlock(byte[] inp ) throws RemoteException{
		ds.hdfs.marshallstuff.FileInfo.Builder newFInfo = ds.hdfs.marshallstuff.FileInfo.newBuilder();
		try {
			ds.hdfs.marshallstuff.FileInfo Inp = ds.hdfs.marshallstuff.FileInfo.parseFrom(inp);
			int chunksNeeded = Inp.getReplication();
			newFInfo.setReplication(chunksNeeded);
			newFInfo.setFilename(Inp.getFilename());
			newFInfo.setFilehandle(Inp.getFilehandle());
			newFInfo.setWritemode(Inp.getWritemode());
		
			if(dataNodeList.size() == 0) {
				System.out.println("No Data Nodes available");
			} else {
				int i = 0;
				while(i < dataNodeList.size() && chunksNeeded > 0) {
					newFInfo.addChunkList(dataNodeList.get(i).ip);  // assigning IP's 
					chunksNeeded--;
					i++;
				}
			}
		}
		catch(Exception e)
		{
			System.err.println("Error at AssignBlock "+ e.toString());
			e.printStackTrace();
			//response.setStatus(-1);
		}
		
		return newFInfo.build().toByteArray();
	}
		
	// 1: "persists the filename" = list of blocks associated with a particular file ?? 
	// 3: list of DataNodes that host replicas of the blocks of the file
	// 4: Gets the list of files in HDFS <<<<<--------------- most likely this 
	public byte[] list(byte[] inp ) throws RemoteException {
		NameSpace.Builder response = NameSpace.newBuilder();
		if(fileInfoList.size() == 0) {
			System.out.println("No files in HDFS");
		}
		try {
			//Implement
			for(FileInfo i : fileInfoList) {
				response.addFilename(i.filename);
			}
		}catch(Exception e) {
			System.err.println("Error at list "+ e.toString());
			e.printStackTrace();
			//response.setStatus(-1);
		}
		return response.build().toByteArray();
	}
	
	/** DataNode <-> NameNode interaction methods **/
		
	// Block reports are sent from the DataNodes along with the heart beat messages
	// >> Used to inform the NameNode about information of the <blocks> in the DataNodes
	public synchronized byte[] blockReport(byte[] inp ) throws RemoteException {
		
		DataNodeInfo.Builder response = DataNodeInfo.newBuilder();
		try {
			/**
			 * Form google protobuf object of data node
			 */
			DataNodeInfo Inp = DataNodeInfo.parseFrom(inp);
			boolean has = false;
			DataNode newNode = new DataNode(Inp.getIp(),Inp.getPort(),Inp.getServerName());
			for(DataNode i : dataNodeList) {
				if(i.ip.equals(newNode.ip)) {
					System.out.println("Data Node is already documented");
					has = true;
				} 
			}
			if(!has) {
				dataNodeList.add(newNode);
			}
			response.setIp(Inp.getIp());
			response.setPort(Inp.getPort());
			response.setServerName(Inp.getServerName());
		}
		catch(Exception e) {
			System.err.println("Error at blockReport "+ e.toString());
			e.printStackTrace();
		}
		// temporary response for the sake of functionality
		return response.build().toByteArray();
	}
	
	
	// Heart beat signals sent from the DataNode to the NameNode
	//   -> BlockReports are tagged along with the heart beat signals 
	public synchronized byte[] heartBeat(byte[] inp ) throws RemoteException {
		// to be sent to each of the DataNodes currently on list
		// -----> temporary response for the sake of functionality
		DataNodeInfo.Builder response = DataNodeInfo.newBuilder();
		try {
			DataNodeInfo Inp = DataNodeInfo.parseFrom(inp);
			response.setIp(Inp.getIp());
			response.setPort(Inp.getPort());
			response.setServerName(Inp.getServerName());
		} catch(Exception e) {
			e.printStackTrace();
		}
		return response.build().toByteArray();
	}
	
	/**
	 * Extra getters and setters and print messages
	 * 
	 */
	public void printMsg(String msg)
	{
		System.out.println(msg);		
	}
	
	protected void setReg(Registry reg) {
		this.serverRegistry = reg;
	}
	protected Registry getReg() {
		return this.serverRegistry;
	}
	
	
	public static void main(String[] args) throws InterruptedException, NumberFormatException, IOException
	{
		// NN just persists the filename, list of blocks associated to that file and its creating time
		// Once DNs send blockReports, NN then knows the locations of each block and tracks this information in memory. 
		/**
         * Server code
         */
        System.setProperty("java.rmi.server.hostname","cd.cs.rutgers.edu");
	
        try {
            NameNode obj = new NameNode("cp.cs.rutgers.edu",1099,"NameNode");
            INameNode stub = (INameNode) UnicastRemoteObject.exportObject(obj, 0);

            Registry registry = LocateRegistry.getRegistry();
  
            registry.bind("NameNode", stub);

            System.err.println("Server ready");
        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
	}
	
}

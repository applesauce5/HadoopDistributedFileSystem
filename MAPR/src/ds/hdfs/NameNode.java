package ds.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FileReader;
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
//import com.testCode.Hello;
//import com.testCode.ImplementHello;
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
	//public static int chunkRep;
  public static LinkedList<DataNode> dataNodeList;
  public static LinkedList<FileInfo> fileInfoList;

	public NameNode(String addr,int p, String nn)
	{
		System.setProperty("java.rmi.server.hostname",addr);
		ip = addr;
		port = p;
		name = nn;
    dataNodeList = new LinkedList<DataNode>();
    fileInfoList = new LinkedList<FileInfo>();
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
	/**
	// Open a file given file name with read-write flag
	boolean findInFilelist(int fhandle) {
		// default until method is filled out
		return false;
	}

	// 2: list of files in HDFS
	public void printFilelist() {
		// default until method is filled out
	}
	**/
	// 1: The client creates a pipeline, after it has info in address info on the DataNode, to the DataNode and writes

	public synchronized byte[] openFile(byte[] inp) throws RemoteException {
		// maybe input stream output stream related
		ds.hdfs.marshallstuff.FileInfo.Builder response = ds.hdfs.marshallstuff.FileInfo.newBuilder();

		try{
			//implement
			ds.hdfs.marshallstuff.FileInfo Inp = ds.hdfs.marshallstuff.FileInfo.parseFrom(inp);
	/**		File file = new File(Inp.getFilename());
			boolean exists = file.exists();

			// If the file exists at all in the Name Node server's file system
			if(file.exists() && file.isFile()) {
**/
				if(Inp.getFilehandle() == 1){ // write protocol
					if(fileInfoList.size() != 0){
						for(FileInfo i : fileInfoList){
							if( (i.filename.equals(Inp.getFilename())) && !(i.writemode) ){ // file is already in hdfs and is not ready to be opened
								System.out.println("File : " +Inp.getFilename()+" cannot be opened ======");
								response.setWritemode(true);
								return response.build().toByteArray(); // send response back to client, not successful
							}
						}
					}
					// File is ready to be used by thread
					FileInfo newFile = new FileInfo(Inp.getFilename(),Inp.getFilehandle(),false,Inp.getReplication());
					fileInfoList.add(newFile); // added to list of files in hdfs

					System.out.println("Creating new file entry in HDFS ======");
					response.setWritemode(false); // this thread has prioty over this file now, only set to true once blocks are persisted
					response.setFilename(Inp.getFilename());
					response.setFilehandle(Inp.getFilehandle()); // some unique id for files?
					response.setReplication(Inp.getReplication());
					return response.build().toByteArray();
					/**for(String i : Inp.getChunkListList()) {
						response.addChunkList(i);
					}**/
				} else if(Inp.getFilehandle() == 0){ // read protocol
					if(fileInfoList.size() == 0){
						System.out.println("Requested file does not exist");
						response.setWritemode(false);
						return response.build().toByteArray(); // send response back to client, not successful
					}
					for(FileInfo i : fileInfoList){
						if((i.filename.equals(Inp.getFilename())) && i.writemode){ // file is already in hdfs and is ready to be opened
							response.setFilename(i.filename);
							response.setFilehandle(i.filehandle);
							response.setWritemode(i.writemode);
							response.setReplication(i.replication);
							response.build().toByteArray(); // send response back to client
						}
					}
				}
			/**} else {
				System.out.println("File does not exist");
				System.err.println("Error: File does not exit");
			}**/
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

	/**
	* Closes file after writing / messing with it
	*/
	public synchronized byte[] closeFile(byte[] inp ) throws RemoteException {
		ds.hdfs.marshallstuff.FileInfo.Builder response = ds.hdfs.marshallstuff.FileInfo.newBuilder();
		try{
			//implement
			ds.hdfs.marshallstuff.FileInfo Inp = ds.hdfs.marshallstuff.FileInfo.parseFrom(inp);
			if(fileInfoList.size() == 0){
				response.setWritemode(false);
				return response.build().toByteArray();
			}
			for(FileInfo i : fileInfoList){
				if((i.filename.equals(Inp.getFilename())) && !(i.writemode)){ // file is already in hdfs and is not ready to be opened
					i.writemode = true;
					response.setWritemode(true);
					response.build().toByteArray(); // send response back to client, not successful
				}
			}
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


	public synchronized byte[] assignBlock(byte[] inp ) throws RemoteException{
		ds.hdfs.marshallstuff.FileInfo.Builder newFInfo = ds.hdfs.marshallstuff.FileInfo.newBuilder();
		//IPList ipInfo = IPList.newBuilder();
		try {
			ds.hdfs.marshallstuff.FileInfo Inp = ds.hdfs.marshallstuff.FileInfo.parseFrom(inp);

			int repFactor = Inp.getReplication();

			newFInfo.setReplication(Inp.getReplication());
			newFInfo.setFilename(Inp.getFilename());
			newFInfo.setFilehandle(Inp.getFilehandle());
			newFInfo.setWritemode(Inp.getWritemode());

			if(dataNodeList.size() == 0) {
				System.out.println("No Data Nodes available");
			} else {
				List<String> list = Inp.getChunkListList();
				System.out.println("Size of incoming chunk list : " + list.size());
				for(int j = 0; j<list.size(); j++){
					int i = 0;
					int k = repFactor;
					StringBuilder chunkInfoBuild = new StringBuilder(500);
					chunkInfoBuild.append(list.get(j)); // name of chunk
					System.out.println("Size of dataNodeList ===============" + dataNodeList.size());
					System.out.println("Replication factor============= " + repFactor);
					while((i < dataNodeList.size()) && (k > 0)) {
						DataNode chosen = dataNodeList.get(i);
						chunkInfoBuild.append(",");
						chunkInfoBuild.append(chosen.serverName);
						chunkInfoBuild.append("|");
						chunkInfoBuild.append(chosen.ip);
						chunkInfoBuild.append("|");
						chunkInfoBuild.append(Integer.toString(chosen.port)); // passing in DataNode information
						k--;
						i++;
					}
					String IpMeta = chunkInfoBuild.toString();// finished assigning ip to chunks
					for(FileInfo file: fileInfoList){
						if(file.filename.equals(Inp.getFilename())){
							file.Chunks.add(IpMeta);
						}
					}
					// Adding final info into response
					newFInfo.addChunkList(IpMeta);  // assigning IP's to the chunks
				}
			}
			return newFInfo.build().toByteArray();
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
	public synchronized byte[] list(byte[] inp ) throws RemoteException {
		NameSpace.Builder response = NameSpace.newBuilder();
		if(fileInfoList.size() == 0) {
			System.out.println("No files in HDFS");
			return response.build().toByteArray();
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
					System.out.println(i.serverName+" already documented");
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
	 * Note: Not used
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
				String configFile = args[0];
				BufferedReader br = new BufferedReader(new FileReader(new File(configFile)));
				String sample = null;
				String NNName = "";
				String ip = "";
				int port = 0;
				//int replication = 0;
				while((sample = br.readLine()) != null) {
					String[] splitSample = sample.split(";");
					NNName = splitSample[0];
					ip = splitSample[1];
					port = Integer.parseInt(splitSample[2]);
					//replication = Integer.parseInt(replication);
				}
				br.close();
				if(NNName.equals("") || ip.equals("") || port == 0 ){ //|| replication == 0){
					System.out.println("Error from reading from Name Node config file");
					return;
				}

        try {
            NameNode obj = new NameNode(ip,port,NNName);
						//chunkRep = replication;
            INameNode stub = (INameNode) UnicastRemoteObject.exportObject(obj, 0);

            Registry registry = LocateRegistry.createRegistry(obj.port);

            registry.rebind("NameNode", stub);

            System.err.println("NameNode Server ready");
        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
	}

}

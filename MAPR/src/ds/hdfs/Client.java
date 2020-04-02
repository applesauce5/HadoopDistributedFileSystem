package ds.hdfs;
import java.net.UnknownHostException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.util.*;
import java.io.*;
//import ds.hdfs.hdfsformat.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
//import ds.hdfs.INameNode;

import ds.hdfs.marshallstuff.*;

/**
 * -> Design the message protocol using protobuf. That will help you in standardizing your work across functions and files. 
 * @author mcho5
 *
 */
public class Client
{
    //Variables Required
    public INameNode NNStub; //Name Node stub
    public IDataNode DNStub; //Data Node stub
    public Client()
    {
        //Get the Name Node Stub
        //nn_details contain NN details in the format Server;IP;Port
    }

    public IDataNode GetDNStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try{
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                IDataNode stub = (IDataNode) registry.lookup(Name);
                return stub;
            }catch(Exception e){
                continue;
            }
        }
    }

    public INameNode GetNNStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try
            {
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                INameNode stub = (INameNode) registry.lookup(Name);
                return stub;
            }catch(Exception e){
                continue;
            }
        }
    }

   
    public void PutFile(String Filename) //Put File ------ incomplete =========================
    {
        System.out.println("Going to put file" + Filename);
      
        try{
        	// Preparing file for export
        	// breaking into chunks
        	File file = new File(Filename);
        	int blockSize = 1000 * 1000 * 64;
        	byte[] buffer = new byte[blockSize];
        	BufferedInputStream BuffIS = new BufferedInputStream(new FileInputStream(file));
        	LinkedList<File> chunkFiles = new LinkedList<File>(); 
        	
        	int bytesAmount = 0;
        	int count = 1;
        	while((bytesAmount = BuffIS.read(buffer)) > 0) {
        		String chunkFile = String.format("%s.%03d", Filename, count++);
        		File newFile = new File(file.getParent(), chunkFile);
        		try(FileOutputStream out = new FileOutputStream(newFile)){
        			out.write(buffer, 0, bytesAmount);
        			out.close();
        		}
        		chunkFiles.add(newFile); // adding on to the list of files
        	}
        	BuffIS.close();
        	
        	//bis = new BufferedInputStream(new FileInputStream(new File(Filename)));
            
        	INameNode tmpNameNode = GetNNStub("NameNode","192.168.1.182",1099); // (name, ip, port);
        	
            // need to consult with NameNode to dismember and allocate blocks
            // eventually refer to the configuration file for parameters
            FileInfo.Builder fileinfo = FileInfo.newBuilder();
        	fileinfo.setReplication(chunkFiles.size()); // number blocks passed in
        	fileinfo.setFilename(Filename);
        	fileinfo.setWritemode(true); // true ?????
        	
        	// --> marshall data into byte array using google protobuf and pass it in as input to the NameNode 
        	byte[] inputInit = fileinfo.build().toByteArray(); // pass in protobuf object
    
        	/**
        	 * Sending to NameNode
        	 */
        	byte[] input = tmpNameNode.openFile(inputInit); // opened the file, received protobuf object
            
        	/**
        	 * Sending to NameNode
        	 */
        	byte[] blkLocations = tmpNameNode.assignBlock(input); // IPs of the replicated Blocks are returned, protobuf object received
            
        	// --> retrieve ip addresses here through google protobuf    
        	FileInfo msgResponse = FileInfo.parseFrom(blkLocations);
            ArrayList<String> list = (ArrayList<String>) msgResponse.getChunkListList();
            
            for(int i = 0; i < list.size(); i++) {
            	IDataNode tmpDataNode = GetDNStub("DataNode",list.get(i),1099); // (name, ip, port)
            	
            	chunkInfo.Builder newchunk = chunkInfo.newBuilder();
            	newchunk.setFilename(i + Filename);
            	
            	byte[] chunk = new byte[(int) chunkFiles.get(i).length()];
            	FileInputStream fis = new FileInputStream(chunkFiles.get(i)); // is this supposed to be protobuf?
            	fis.read(chunk);
            	fis.close();
            	newchunk.setFileData(ByteString.copyFrom(chunk)); // File --> byte[] --> ByteString
            	
            	byte[] insertchunk = newchunk.build().toByteArray();
            	
            	/**
            	 * Sending to DataNode
            	 */
            	tmpDataNode.writeBlock(insertchunk); // passing in by chunk by chunk to the DataNodes	
            }
            
            byte[] doneWrite = tmpNameNode.closeFile(input);
            // Done with writing chunks to their respective DataNodes
        }catch(Exception e){
            System.out.println("File not found !!!");
            return;
        }
    }

    public void GetFile(String Filename) throws IOException {
    	 // need to consult with NameNode to dismember and allocate blocks
        // eventually refer to the configuration file for parameters
        INameNode tmpNameNode = GetNNStub("NameNode","192.168.1.182",1099); // (name, ip, port);
        
        FileInfo.Builder fileinfo = FileInfo.newBuilder();
        fileinfo.setFilename(Filename);
        
    	byte[] inputInit = fileinfo.build().toByteArray(); // pass in protobuf object
    	byte[] input = tmpNameNode.openFile(inputInit); // opened the file, received protobuf object
        
    	/**
    	 * Sending to Name Node
    	 */
        byte[] byteResInfo = tmpNameNode.getBlockLocations(input); // IPs of DataNode are given
    	
        FileInfo resInfo = FileInfo.parseFrom(byteResInfo);
    	ArrayList<String> list = (ArrayList<String>) resInfo.getChunkListList();
    	
        // Go to the Data Nodes to retrieve the blocks and read from each in sequence to combine them
    	
    	LinkedList<File> chunkList = new LinkedList<File>();
    	
    	for(int i = 0; i < list.size(); i++) {
    		IDataNode tmpDataNode = GetDNStub("DataNode", list.get(i),1099);
    		chunkInfo.Builder newchunk = chunkInfo.newBuilder();
        	newchunk.setFilename(i + Filename);
        	
        	byte[] readchunk = newchunk.build().toByteArray();
        	
        	/**
        	 * Sending to DataNode
        	 */
        	byte[] resByte = tmpDataNode.readBlock(readchunk);
        	chunkInfo res = chunkInfo.parseFrom(resByte);
        	ByteString fileByteStr = res.getFileData();
        	
        	File newfile = new File(Filename);
        	fileByteStr.writeTo(new FileOutputStream(newfile)); // writing contents of bytestring to the new file
        	chunkList.add(newfile);
    	}
    	
    	// reading into final file
    	
    	File finalFile = new File(Filename);
    	OutputStream output = new BufferedOutputStream(new FileOutputStream(finalFile, true));
    	for(File src : chunkList) {
    		InputStream is = new BufferedInputStream(new FileInputStream(src));
    		byte[] tmpbuff = new byte[1024*4];
            int n = 0;
            while (-1 != (n = is.read(tmpbuff))) {
                output.write(tmpbuff, 0, n);
            }
            is.close();		
    	}
    	output.close();
    	
    	// Finished reading into file
    }

    public void List() // list all the files in present in HDFS ============================
    {
    	  INameNode tmpNameNode = GetNNStub("NameNode","192.168.1.182",1099); // (name, ip, port);
    }

    /**
     *  -> Clients and DN discover the NN from a conf file 
     *  and read from a standardized location (/tmp/somefile). 
     *  
     *  -> The conf file contains the socket information of the NN (port number). 
     * @param args
     * @throws RemoteException
     * @throws UnknownHostException
     */
    public static void main(String[] args) throws RemoteException, UnknownHostException
    {
        // To read config file and Connect to NameNode
        //Intitalize the Client
        Client Me = new Client();
        System.out.println("Welcome to HDFS!!");
        Scanner Scan = new Scanner(System.in);
        while(true)
        {
            //Scanner, prompt and then call the functions according to the command
            System.out.print("$> "); //Prompt
            String Command = Scan.nextLine();
            String[] Split_Commands = Command.split(" ");

            if(Split_Commands[0].equals("help"))
            {
                System.out.println("The following are the Supported Commands");
                System.out.println("1. put filename ## To put a file in HDFS");
                System.out.println("2. get filename ## To get a file in HDFS"); System.out.println("2. list ## To get the list of files in HDFS");
            }
            else if(Split_Commands[0].equals("put"))  // put Filename
            {
                //Put file into HDFS
                String Filename;
                try{
                    Filename = Split_Commands[1];
                    Me.PutFile(Filename);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                    continue;
                }
            }
            else if(Split_Commands[0].equals("get"))
            {
                //Get file from HDFS
                String Filename;
                try{
                    Filename = Split_Commands[1];
                    Me.GetFile(Filename);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                    continue;
                } catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
            else if(Split_Commands[0].equals("list"))
            {
                System.out.println("List request");
                //Get list of files in HDFS
                Me.List();
            }
            else
            {
                System.out.println("Please type 'help' for instructions");
            }
        }
    }
}

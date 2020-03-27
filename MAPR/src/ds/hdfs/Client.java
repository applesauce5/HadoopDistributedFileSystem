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
import com.google.protobuf.Parser;
//import ds.hdfs.INameNode;

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
        BufferedInputStream bis;
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
            
            // need to consult with NameNode to dismember and allocate blocks
            // eventually refer to the configuration file for parameters
           
        	INameNode tmpNameNode = GetNNStub("NameNode","192.168.1.182",1099); // (name, ip, port);
            IDataNode tmpDataNode = GetDNStub();
    
        	//byte[] input = ByteString.readFrom(new BufferedInputStream(new FileInputStream(chunkFiles.get(i)))).toByteArray();
            
        	// --> marshall data into byte array using google protobuf and pass it in as input to the NameNode 
        	byte[] blkLocations = tmpNameNode.assignBlock(input); // IPs of the replicated Blocks are returned
            // --> retrieve ip addresses here through google protobuf 
            
            
            
        }catch(Exception e){
            System.out.println("File not found !!!");
            return;
        }
    }

    public void GetFile(String FileName) // Get File ------ incomplete =========================
    {
    	 // need to consult with NameNode to dismember and allocate blocks
        // eventually refer to the configuration file for parameters
        INameNode tmpNameNode = GetNNStub("NameNode","192.168.1.182",1099); // (name, ip, port);
        
        byte[] fileInfo = tmpNameNode.getBlockLocations(input);
    	// get the file ? 
        
        // Go to the Data Nodes to retrieve the blocks and read from each in sequence to combine them
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

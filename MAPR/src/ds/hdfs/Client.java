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
import ds.hdfs.marshallstuff.FileInfo;
import ds.hdfs.marshallstuff.chunkInfo;
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
    public static int chunkRep;
    public static int blkSize;

    public Client(int cRep,int bSize){
        //Get the Name Node Stub
        //nn_details contain NN details in the format Server;IP;Port
        chunkRep = cRep;
        blkSize = bSize;
        //this.NNStub = NNStub;
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

    public INameNode GetNNStub(String Name, String IP, int Port){
        while(true) {
            try {
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                INameNode stub = (INameNode) registry.lookup(Name);
                return stub;
            }catch(Exception e){
                continue;
            }
        }
    }


    public synchronized void PutFile(String Filename) {
        System.out.println("Going to put file" + Filename);
        try{
        	/**
        	// Preparing file for export
        	// breaking into 64MB chunks
        	 *
        	 */
        	File file = new File(Filename);
        	//int blockSize = 1000 * 1000 * 64; // 64 MB
          int blockSize = blkSize; // 40
          byte[] buffer = new byte[blockSize];
        	BufferedInputStream BuffIS = new BufferedInputStream(new FileInputStream(file));
        	LinkedList<File> chunkFiles = new LinkedList<File>();  // List of chunk files

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

        	// Reference to the Name Node stub
        	INameNode tmpNameNode = this.NNStub;//GetNNStub("NameNode","cp.cs.rutgers.edu",2002);

        	/**
            // need to consult with NameNode to  allocate blocks
            // eventually refer to the configuration file for parameters
             *
             */
          FileInfo.Builder fileinfo = FileInfo.newBuilder();
          fileinfo.setFilename(Filename);
        	fileinfo.setReplication(chunkRep); // replication factor of the blocks to be passed to dataNodes
          fileinfo.setFilehandle(1); // 0 for read, 1 for write
        	//fileinfo.setWritemode(true);

        	// --> marshall data into byte array using google protobuf and pass it in as input to the NameNode
        	byte[] inputInit = fileinfo.build().toByteArray(); // pass in protobuf object

        	/**
        	 * Sending to NameNode
        	 */
        	byte[] input = tmpNameNode.openFile(inputInit); // opened the file, received protobuf object
          FileInfo in = FileInfo.parseFrom(input);
          FileInfo.Builder openFlResponse = FileInfo.newBuilder();
          openFlResponse.setFilename(in.getFilename());
          openFlResponse.setFilehandle(in.getFilehandle());
          openFlResponse.setWritemode(in.getWritemode());
          openFlResponse.setReplication(in.getReplication());
          openFlResponse.addAllChunkList(in.getChunkListList());

          if(openFlResponse.getWritemode() == false){
            System.out.println("Cannot access file now");
            return; // writemode was equal false
          }

          // Now have a list of chunknames to pass onto NameNode
          for(int i = 0; i< chunkFiles.size(); i++){
            String chunkName = i+Filename;
            openFlResponse.addChunkList(chunkName);
          }
          // Sending to NameNode to assign blocks for a particular file
          byte[] blkLocations = tmpNameNode.assignBlock(input); // IPs of the replicated Blocks are returned, protobuf object received --> stored in Chunks data structure

          // extract ip addresses here
          FileInfo msgResponse = FileInfo.parseFrom(blkLocations);

          ArrayList<String> list = (ArrayList<String>) msgResponse.getChunkListList();

          // Going through each chunk from large file
          for(int i = 0; i < list.size(); i++) {
              String[] splitPhrase = list.get(i).split(",",-1);
              String chunkName = splitPhrase[0];
              // distribute chunk replicas among given DataNodes
              for(int k = 1; k < splitPhrase.length;k++){
                String[] dataNodeMeta = splitPhrase[k].split("|");

              	IDataNode tmpDataNode = GetDNStub(dataNodeMeta[0],dataNodeMeta[1],Integer.parseInt(dataNodeMeta[2])); // (name, ip, port)

              	chunkInfo.Builder newchunk = chunkInfo.newBuilder();
              	newchunk.setFilename(chunkName);

              	// File --> byte[]
              	byte[] chunk = new byte[(int) chunkFiles.get(i).length()];
              	FileInputStream fis = new FileInputStream(chunkFiles.get(i));
              	fis.read(chunk);
              	fis.close();
              	newchunk.setFileData(ByteString.copyFrom(chunk)); // File --> byte[] --> ByteString

              	byte[] insertchunk = newchunk.build().toByteArray();
              	 // Sending to DataNode
              	 // -> passing in by chunk by chunk to the DataNodes	to the ip addresses given
              	tmpDataNode.writeBlock(insertchunk); //
              }
          }

          /**
           * Finally close the file
           */
          byte[] doneWrite = tmpNameNode.closeFile(input);
          FileInfo resWrite = FileInfo.parseFrom(doneWrite);
          if(!(resWrite.getWritemode())){
            System.out.println("Error closing file; Error persisting file");
            return;
          }
            // Done with writing chunks to their respective DataNodes
        }catch(Exception e){
            System.out.println("File not found !!!");
            return;
        }
    }

    public void GetFile(String Filename) throws IOException {
    	 // need to consult with NameNode to dismember and allocate blocks
        // eventually refer to the configuration file for parameters
        INameNode tmpNameNode = this.NNStub;//GetNNStub("NameNode","cp.cs.rutgers.edu",2002); // (name, ip, port);

        FileInfo.Builder fileinfo = FileInfo.newBuilder();
        fileinfo.setFilename(Filename);
        fileinfo.setReplication(chunkRep); // replication factor of the blocks
        fileinfo.setFilehandle(0); // 0 for read, 1 for write

        // --> marshall data into byte array using google protobuf and pass it in as input to the NameNode
        byte[] inputInit = fileinfo.build().toByteArray(); // pass in protobuf object

        byte[] input = tmpNameNode.openFile(inputInit); // opened the file, received protobuf object
        FileInfo openInfo = FileInfo.parseFrom(input);
        // The file does not exist in hdfs
        if(openInfo.getWritemode() == false){
          System.out.println("Cannot access file now or file does not exist in hdfs");
          return; // writemode was equal false
        }

      	/**
      	 * Sending to Name Node
      	 */
        byte[] byteResInfo = tmpNameNode.getBlockLocations(input); // IPs of DataNode are given

        FileInfo resInfo = FileInfo.parseFrom(byteResInfo);
  	   ArrayList<String> list = (ArrayList<String>) resInfo.getChunkListList();

        // Go to the Data Nodes to retrieve the blocks and read from each in sequence to combine them

    	LinkedList<File> chunkList = new LinkedList<File>();

    	for(int i = 0; i < list.size(); i++) {
          String[] splitPhrase = list.get(i).split(",",-1);
          String chunkName = splitPhrase[0];

          String[] dataNodeMeta = splitPhrase[1].split("|");
    		  IDataNode tmpDataNode = GetDNStub(dataNodeMeta[0], dataNodeMeta[1],Integer.parseInt(dataNodeMeta[2])); // So far, can only handle reading from the same DataNode

          chunkInfo.Builder newchunk = chunkInfo.newBuilder();
        	newchunk.setFilename(chunkName);

        	byte[] readchunk = newchunk.build().toByteArray();

        	/**
        	 * Sending to DataNode, read a block from each DataNode and put inside a File Linked List
        	 */
        	byte[] resByte = tmpDataNode.readBlock(readchunk);
        	ds.hdfs.marshallstuff.chunkInfo res = ds.hdfs.marshallstuff.chunkInfo.parseFrom(resByte);
        	ByteString fileByteStr = res.getFileData();

        	File newfile = new File(Filename);
        	fileByteStr.writeTo(new FileOutputStream(newfile)); // writing contents of bytestring to the new file
        	chunkList.add(newfile);
    	}

    	/**
    	 *  Combine all the chunks read into 1 file
    	 */
    	File finalFile = new File(Filename);
    	OutputStream output = new BufferedOutputStream(new FileOutputStream(finalFile, true));
    	for(File src : chunkList) {
    		InputStream is = new BufferedInputStream(new FileInputStream(src));
    		byte[] tmpbuff = new byte[blkSize];
            int n = 0;
            while (-1 != (n = is.read(tmpbuff))) {
                output.write(tmpbuff, 0, n);
            }
            is.close();
    	}
    	output.close();

    	// Finished reading into file
    }

    public void List() throws RemoteException {
    	  INameNode tmpNameNode = this.NNStub;//GetNNStub("NameNode","cp.cs.rutgers.edu",2002); // (name, ip, port);

    	  NameSpace.Builder input = NameSpace.newBuilder(); // placebo input
    	  byte[] res = tmpNameNode.list(input.build().toByteArray());

    	  try {
    		  NameSpace listOfFilesByte = NameSpace.parseFrom(res);
	    	  LinkedList<String> listFiles = (LinkedList<String>) listOfFilesByte.getFilenameList();
          if(listFiles.size()==0){
            System.out.println("No files in HDFS");
            return;
          }
	    	  for(String i : listFiles) {
	    		  System.out.println(i);
	    	  }
  		  } catch (InvalidProtocolBufferException e) {
  			  // TODO Auto-generated catch block
  			  System.out.println("error in parsing info");
  			  e.printStackTrace();
  		  }
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
    public static void main(String[] args) throws RemoteException, UnknownHostException, FileNotFoundException, IOException
    {
        // To read config file and Connect to NameNode
        //Intitalize the Client
        String configFile = args[0];
        String NNconfigFile = args[1];

        BufferedReader configReader = new BufferedReader(new FileReader(new File(configFile)));
        String sample = null;
        int chunkSz = 0;
        int rep = 0;
        while((sample = configReader.readLine()) != null) {
          String[] splitSample = sample.split(":");
          if(splitSample[0].equals("size")){
            chunkSz = Integer.parseInt(splitSample[1]);
          } else if(splitSample[1].equals("replication")){
            rep = Integer.parseInt(splitSample[1]);
          }
        }
        configReader.close();

        BufferedReader NNconfigReader = new BufferedReader(new FileReader(new File(NNconfigFile)));
        sample = NNconfigReader.readLine();
        String[] splitSample = sample.split(";");

        NNconfigReader.close();

        Client Me = new Client(rep,chunkSz);//,serverNode);
        Me.NNStub = Me.GetNNStub(splitSample[0],splitSample[1],Integer.parseInt(splitSample[2]));

        // Start of application
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

package com.testCode;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import com.testCode.Hello;
public class Server implements Hello {
        
    public Server() {}

//    protected Server() throws RemoteException {
//		super();
//		// TODO Auto-generated constructor stub
//	}
    public String sayHello() {
        return "Hello, world!";
    }
        
    public static void main(String args[]) {
    	System.setProperty("java.rmi.server.hostname","192.168.1.182");
//    	System.setProperty("java.security.policy","test.policy");
//    	if (System.getSecurityManager() == null) {
//            System.setSecurityManager(new SecurityManager());
//        }
//    	
        try {
            Server obj = new Server();
            Hello stub = (Hello) UnicastRemoteObject.exportObject(obj, 0);

            // Bind the remote object's stub in the registry
              Registry registry = LocateRegistry.getRegistry();
            //Naming.lookup("rmi://localhost:1099/Server");
           // Naming.rebind("HelloServer", obj);
              registry.bind("Hello", stub);

            System.err.println("Server ready");
        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }
}

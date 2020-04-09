package com.testCode;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import com.testCode.Hello;
public class Server implements Hello {

    public Server() {
      //System.setProperty("java.security.policy","test.policy");
    //  System.setProperty("java.rmi.server.hostname","128.6.13.171");
      System.setProperty("java.rmi.server.hostname","ls.cs.rutgers.edu");
    }

//    protected Server() throws RemoteException {
//		super();
//		// TODO Auto-generated constructor stub
//	}
    public String sayHello() {
        return "Hello, world!";
    }

    public static void main(String args[]) {



        try {
            Server obj = new Server();
            Hello stub = (Hello) UnicastRemoteObject.exportObject(obj, 0);

            // Bind the remote object's stub in the registry
              Registry registry = LocateRegistry.createRegistry(2002);
            //Naming.lookup("rmi://localhost:1099/Server");
           // Naming.rebind("HelloServer", obj);

              registry.rebind("Hello", stub);
            //registry.rebind("Hello",stub);
            System.err.println("Server ready");
        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }
}

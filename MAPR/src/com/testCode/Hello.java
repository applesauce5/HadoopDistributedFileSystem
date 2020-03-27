//package com.testCode;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Hello extends Remote,java.io.Serializable {
    String sayHello() throws RemoteException;
}
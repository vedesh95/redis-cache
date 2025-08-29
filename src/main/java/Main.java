import struct.ServerInfo;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args){
        int port = 6379;
        if(args.length>=2 && args[0].equalsIgnoreCase("--port")) port = Integer.parseInt(args[1]);
        // args as --port 6380 --replicaof "localhost 6379"
        boolean isreplica = false;
        if(args.length>=4 && args[2].equalsIgnoreCase("--replicaof")) isreplica = true;
        Cache cache = new Cache();
        if(isreplica) cache.getInfo().setRole("slave");

        ServerSocket serverSocket = null;

        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            while (true){
                Socket clientSocket = serverSocket.accept();
                cache.addClient(clientSocket);
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}


import struct.ServerInfo;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args){
        int port = 6379;
        if(args.length>=2 && args[0].equalsIgnoreCase("--port")) port = Integer.parseInt(args[1]);

        Cache cache = new Cache();
        ServerSocket serverSocket = null;

        if(args.length>=4 && args[2].equalsIgnoreCase("--replicaof")){
            port = Integer.parseInt(args[1]);
            String[] address = args[3].split(" ");
            try{
                Socket slave = new Socket(address[0], Integer.parseInt(address[1])); // to connect to master
                slave.setReuseAddress(true);
                slave.getOutputStream().write("*1\r\n$4\r\nPING\r\n".getBytes());
                slave.getOutputStream().flush();
                // REPLCONF listening-port <PORT>
                slave.getOutputStream().write(("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + String.valueOf(port).length() + "\r\n" + port + "\r\n").getBytes());
                slave.getOutputStream().flush();
                // REPLCONF capa eof
                slave.getOutputStream().write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".getBytes());
                slave.getOutputStream().flush();
                slave.close();
                cache.getInfo().setRole("slave");
            }catch (Exception e){
                System.out.println("Failed to connect to master: " + e.getMessage());
                return;
            }
        }

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


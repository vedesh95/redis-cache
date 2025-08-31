import struct.ClientType;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args){
        int port = 6379;
        RedisCache cache = new RedisCache();
        ServerSocket serverSocket = null;
        Socket slave = null;

        if(args.length>=2 && args[0].equalsIgnoreCase("--port")) port = Integer.parseInt(args[1]);
        for(int i=0; i<args.length; i++){
            if(args[i].equalsIgnoreCase("--dir")) cache.getRdbDetails().setDir(args[i+1]);
            if(args[i].equalsIgnoreCase("--dbfilename")) cache.getRdbDetails().setDbfilename(args[i+1]);
        }
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
        } catch (Exception e) {
            System.out.println("IOException: " + e);
        }

        if(args.length>=4 && args[2].equalsIgnoreCase("--replicaof")){
            port = Integer.parseInt(args[1]);
            String[] address = args[3].split(" ");
            try{
                slave = new Socket(address[0], Integer.parseInt(address[1])); // to connect to master
                slave.setReuseAddress(true);
                slave.getOutputStream().write("*1\r\n$4\r\nPING\r\n".getBytes());
                slave.getOutputStream().flush();

                // wait for +PONG
                BufferedReader reader = new BufferedReader(new InputStreamReader(slave.getInputStream()));
                String response = reader.readLine();
                if(!response.equals("+PONG")){
                    System.out.println("Failed to connect to master: " + response);
                    return;
                }
                // REPLCONF listening-port <PORT>
                slave.getOutputStream().write(("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + String.valueOf(port).length() + "\r\n" + port + "\r\n").getBytes());
                slave.getOutputStream().flush();
                response = reader.readLine();
                if(!response.equals("+OK")){
                    System.out.println("Failed to connect to master: " + response);
                    return;
                }
                // REPLCONF capa eof
                slave.getOutputStream().write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".getBytes());
                slave.getOutputStream().flush();
                response = reader.readLine();
                if(!response.equals("+OK")){
                    System.out.println("Failed to connect to master: " + response);
                    return;
                }
                // PSYNC ?
                System.out.println("Connected to master, sending PSYNC");
                slave.getOutputStream().write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".getBytes());
                slave.getOutputStream().flush();
                reader.readLine();

                cache.getInfo().setRole("slave");
                cache.addClient(slave, ClientType.DBCLIENT, reader, slave.getOutputStream());
            }catch(Exception e){
                System.out.println("Failed to connect to master: " + e.getMessage());
            }
        }

        try {
            while (true){
                Socket clientSocket = serverSocket.accept();
                cache.addClient(clientSocket, ClientType.NONDBCLIENT, new BufferedReader(new InputStreamReader(clientSocket.getInputStream())), clientSocket.getOutputStream());
            }
        } catch (Exception e) {
            System.out.println("IOException: " + e);
        }
    }
}


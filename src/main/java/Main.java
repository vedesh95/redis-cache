import command.Command;
import struct.ServerInfo;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class Main {
    public static void main(String[] args){
        int port = 6379;
        if(args.length>=2 && args[0].equalsIgnoreCase("--port")) port = Integer.parseInt(args[1]);

        Cache cache = new Cache();
        ServerSocket serverSocket = null;
        Socket slave = null;
        String serveraddress = null;

        if(args.length>=4 && args[2].equalsIgnoreCase("--replicaof")){
            port = Integer.parseInt(args[1]);
            String[] address = args[3].split(" ");
            try{
                serveraddress = address[0];
                slave = new Socket(address[0], Integer.parseInt(address[1])); // to connect to master
                slave.setReuseAddress(true);
                slave.getOutputStream().write("*1\r\n$4\r\nPING\r\n".getBytes());
                slave.getOutputStream().flush();
                // wait for +PONG
                BufferedReader reader = new BufferedReader(new InputStreamReader(slave.getInputStream()));
                String response = reader.readLine();
                System.out.println("response to ping: " + response);
                if(!response.equals("+PONG")){
                    System.out.println("Failed to connect to master: " + response);
                    return;
                }
                // REPLCONF listening-port <PORT>
                slave.getOutputStream().write(("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + String.valueOf(port).length() + "\r\n" + port + "\r\n").getBytes());
                slave.getOutputStream().flush();
                // check response
                response = reader.readLine();
                if(!response.equals("+OK")){
                    System.out.println("Failed to connect to master: " + response);
                    return;
                }
                // REPLCONF capa eof
                slave.getOutputStream().write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".getBytes());
                slave.getOutputStream().flush();
                // check response
                response = reader.readLine();
                if(!response.equals("+OK")){
                    System.out.println("Failed to connect to master: " + response);
                    return;
                }
                // PSYNC ?
                slave.getOutputStream().write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".getBytes());
                slave.getOutputStream().flush();
                // check response
                response = reader.readLine();

                cache.getInfo().setRole("slave");
//                cache.addClient(slave);

//                Thread.sleep(1000);
                Object cmd = Client.parseRESP(slave.getInputStream());
                System.out.println(cmd.toString());
//                if(repl.contains("REPLCONF") && repl.contains("GETACK")){
//                    slave.getOutputStream().write("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n".getBytes());
//                    slave.getOutputStream().flush();
//                }

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


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
//                String cmd = Client.parseBulkString(reader);
//                System.out.println(cmd);
//                if(repl.contains("REPLCONF") && repl.contains("GETACK")){
//                    slave.getOutputStream().write("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n".getBytes());
//                    slave.getOutputStream().flush();
//                }
                // reader can contains bulk strings now. read string from reader until null
//                reader.readLine();
//                reader.readLine();
//                reader.readLine();
//                System.out.println(reader.readLine());
//                System.out.println(reader.readLine());
//                System.out.println(reader.readLine());
//                System.out.println(reader.readLine());
//                System.out.println(reader.readLine());
//                System.out.println(reader.readLine());

                // first replconf
                // check if readline contains REPLCONF
                String line = reader.readLine();
                if(line!=null && line.contains("$")){
                    // read next line
                    for(int i=0;i<8;i++) reader.readLine();
                    slave.getOutputStream().write("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n".getBytes());
                    slave.getOutputStream().flush();
                }

                cache.addClient(slave);
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

    public static void readBulkStrings(BufferedReader reader) throws IOException {
        // read a series of bulk strings from reader until null
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("$")) {
                int length = Integer.parseInt(line.substring(1));
                if (length == -1) {
                    System.out.println("null");
                } else {
                    char[] chars = new char[length];
                    reader.read(chars, 0, length);
                    reader.readLine(); // read the trailing \r\n
                    String bulkString = new String(chars);
//                    System.out.println(bulkString);
                }
            } else {
                System.out.println("Unexpected line: " + line);
            }
        }
    }



}


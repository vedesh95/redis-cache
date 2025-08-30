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
                readBulkStrings(reader);
                slave.getOutputStream().write("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n".getBytes());
                slave.getOutputStream().flush();
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
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("$")) {
                int len = Integer.parseInt(line.substring(1));
                if (len == -1) continue; // Null bulk string
                char[] buf = new char[len];
                int read = 0;
                while (read < len) {
                    int r = reader.read(buf, read, len - read);
                    if (r == -1) throw new IOException("Bulk string length mismatch");
                    read += r;
                }
                // Consume trailing \r\n
                for (int i = 0; i < 2; i++) {
                    if (reader.read() == -1) throw new IOException("Unexpected end of stream after bulk string");
                }
                String bulk = new String(buf);
                System.out.println("Bulk string: " + bulk);
            } else {
                // Handle other RESP types or break
                break;
            }
        }
    }



}


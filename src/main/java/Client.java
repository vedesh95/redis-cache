import command.Command;
import struct.KeyValue;
import struct.Pair;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Client {
    private CommandHandler commandHandler;
    private final Socket clientSocket;
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap = new ConcurrentHashMap<>();
    private List<List<String> > transaction;
    private Map<Socket, Integer> slaves;

    public Client(CommandHandler commandHandler, Socket clientSocket, ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap, Map<Socket, Integer> slaves) {
        this.commandHandler = commandHandler;
        this.clientSocket = clientSocket;
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
        this.transaction = new ArrayList<>();
        this.slaves = slaves;
    }

    public void listen() {
        try (clientSocket; BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())); OutputStream out = clientSocket.getOutputStream()) {
            boolean isInTransaction = false;
            while(true){
                List<String> command = parseCommand(reader);
                if(command.isEmpty()) continue;
                System.out.println("Command received: " + command);
                if(command.get(0).equalsIgnoreCase("MULTI")){
                    isInTransaction = true;
                    out.write("+OK\r\n".getBytes());
                    out.flush();
                }else if(command.get(0).equalsIgnoreCase("EXEC")){
                    if(!isInTransaction){ // handle case for exec without multi
                        out.write("-ERR EXEC without MULTI\r\n".getBytes());
                        out.flush();
                        continue;
                    }
                    isInTransaction = false;
                    if(transaction.isEmpty()){
                        // reply with empty array
                        out.write("*0\r\n".getBytes());
                        out.flush();
                        continue;
                    }
                    out.write(("*" + transaction.size() + "\r\n").getBytes());
                    out.flush();
                    for(List<String> cmd : transaction){
                        this.commandHandler.handleCommand(cmd, out);
                    }
                    transaction.clear();
//                    out.write("+OK\r\n".getBytes());
//                    out.flush();
                }else if(command.get(0).equalsIgnoreCase("DISCARD")){
                    if(!isInTransaction){ // handle case for exec without multi
                        out.write("-ERR DISCARD without MULTI\r\n".getBytes());
                        out.flush();
                        continue;
                    }
                    isInTransaction = false;
                    transaction.clear();
                    out.write("+OK\r\n".getBytes());
                    out.flush();

                }else if(isInTransaction) {
                    transaction.add(command);
                    out.write("+QUEUED\r\n".getBytes());
                    out.flush();
                }
                else this.commandHandler.handleCommand(command, out);

//                System.out.println("Received command: " + command);
                // if command is PSYNC or SYNC, set propogateToSlaves to true
                if(command.get(0).equalsIgnoreCase("PSYNC") || command.get(0).equalsIgnoreCase("SYNC")){
                    // add clientSocket to slaves map with value 1
                    this.slaves.put(clientSocket, 1);
                    System.out.println("Added slave: " + clientSocket);

                }
                // propogate command to all slaves through their sockets
                for(Socket socket : slaves.keySet()){
                    this.commandHandler.propagateToSlaves(command, socket.getOutputStream());
                }
            }
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    public static List<String> parseCommand(BufferedReader reader) throws IOException {
        String line;
        line = reader.readLine();
        List<String> command = new java.util.ArrayList<>();
        if (line != null && line.startsWith("*")) {
            int n = Integer.parseInt(line.substring(1));
            for (int i = 0; i < n; i++) {
                line = reader.readLine();
                line = reader.readLine();
                command.add(line);
            }
        }
        return command;
    }

    public static Object parseRESP(InputStream in) throws IOException {
        int type = in.read();
        if (type == -1) return null;

        String line = readLine(in);
        switch (type) {
            case '+': // Simple String
            case '-': // Error
            case ':': // Integer
                return line;
            case '$': { // Bulk String
                int length = Integer.parseInt(line);
                if (length == -1) return null;
                byte[] buf = new byte[length];
                int read = 0;
                while (read < length) {
                    int r = in.read(buf, read, length - read);
                    if (r == -1) throw new IOException("Incomplete bulk string");
                    read += r;
                }
                readLine(in); // consume \r\n
                return new String(buf);
            }
            case '*': { // Array
                int count = Integer.parseInt(line);
                List<Object> arr = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    arr.add(parseRESP(in));
                }
                return arr;
            }
            default:
                throw new IOException("Unknown RESP type: " + (char)type);
        }
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                in.read(); // consume \n
                break;
            }
            baos.write(b);
        }
        return baos.toString();
    }
}

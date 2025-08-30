import command.Command;
import struct.ClientType;
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
    private ClientType clientType;

    public Client(CommandHandler commandHandler, ClientType clientType, Socket clientSocket, ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap, Map<Socket, Integer> slaves) {
        this.commandHandler = commandHandler;
        this.clientSocket = clientSocket;
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
        this.transaction = new ArrayList<>();
        this.slaves = slaves;
        this.clientType = clientType;
    }

    public void listen(Socket clientSocket, BufferedReader reader, OutputStream out) {

        try (clientSocket; reader; out) {
            boolean isInTransaction = false;

            List<List<String>> lastcommands = new ArrayList<>();
            List<Integer> lastcommandsBytes = new ArrayList<>();

            while(true){
                List<String> command = new ArrayList<>();
                command = parseCommand(reader);

                if(command.isEmpty()) continue;
                lastcommands.add(command);

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
                } else if(command.get(0).equalsIgnoreCase("REPLCONF")){
                     for(List<String> cmd : lastcommands){
                        StringBuilder sb = new StringBuilder();
                        sb.append("*").append(cmd.size()).append("\r\n");
                        for(String arg : cmd){
                            sb.append("$").append(arg.length()).append("\r\n");
                            sb.append(arg).append("\r\n");
                        }
                        lastcommandsBytes.add(sb.toString().getBytes().length);
                    }
                    lastcommands.clear();
                    // calculate sum of elements in lastcommandsBytes
                    int totalBytes = lastcommandsBytes.stream().mapToInt(Integer::intValue).sum();
                    out.write(("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + String.valueOf(totalBytes).length() + "\r\n" + totalBytes + "\r\n").getBytes());
                    out.flush();
                }else {
                    System.out.println("Received command: " + command + " from " + clientSocket);
                    if(this.clientType == ClientType.NONDBCLIENT) this.commandHandler.handleCommand(command, out);
                    else this.commandHandler.handleCommand(command, new OutputStream() {
                        @Override
                        public void write(int b) throws IOException {}
                    });
                }

                if(command.get(0).equalsIgnoreCase("PSYNC") || command.get(0).equalsIgnoreCase("SYNC")){
                    this.slaves.put(clientSocket, 1);
                    System.out.println("Added slave: " + clientSocket);
                }

                for(Socket socket : slaves.keySet()){
                    this.commandHandler.propagateToSlaves(command, socket.getOutputStream());
                }
            }
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public static List<String> parseCommand(BufferedReader reader) throws IOException {
        String line;
        line = reader.readLine();
        System.out.println("Parsing line: " + line);
        List<String> command = new java.util.ArrayList<>();
        if (line != null && line.startsWith("*")) {
            int n = Integer.parseInt(line.substring(1));
            for (int i = 0; i < n; i++) {
                line = reader.readLine();
                line = reader.readLine();
                command.add(line);
            }
        }
        // if line starts with $ ex $x then fetch next x bytes
        else if (line != null && line.startsWith("$")) {
            int n = Integer.parseInt(line.substring(1));
            char[] buf = new char[n];
            reader.read(buf, 0, n);
            // build string from buf
            String parsedline = new String(buf);
            reader.readLine(); // read the trailing \r\n
            if(parsedline == "REPLCONF"){
                command.add(parsedline);
                n = Integer.parseInt(line.substring(1));
                buf = new char[n];
                reader.read(buf, 0, n);
                reader.readLine(); // read the trailing \r\n
                parsedline = new String(buf);
                command.add(parsedline);
                n = Integer.parseInt(line.substring(1));
                buf = new char[n];
                reader.read(buf, 0, n);
                reader.readLine(); // read the trailing \r\n
                parsedline = new String(buf);
                command.add(parsedline);
            }
            System.out.println("Parsed command: " + command);
        }
        return command;
    }
}

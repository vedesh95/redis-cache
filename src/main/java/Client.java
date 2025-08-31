import command.Command;
import struct.ClientType;
import struct.KeyValue;
import struct.Pair;
import struct.SlaveDetails;

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
    private Map<Socket, SlaveDetails> slaves;
    private ClientType clientType;

    public Client(CommandHandler commandHandler, ClientType clientType, Socket clientSocket, ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap, Map<Socket, SlaveDetails> slaves) {
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

                if(!lastcommands.isEmpty() && !command.get(0).equalsIgnoreCase("REPLCONF"))  lastcommands.add(command);

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
                } else if(command.get(0).equalsIgnoreCase("REPLCONF") && command.get(1).equalsIgnoreCase("GETACK") && command.get(2).equals("*")){
                    // handle REPLCONF GETACK *
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
                    lastcommands.add(command);
                    // calculate sum of elements in lastcommandsBytes
                    int totalBytes = lastcommandsBytes.stream().mapToInt(Integer::intValue).sum();
                    out.write(("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + String.valueOf(totalBytes).length() + "\r\n" + totalBytes + "\r\n").getBytes());
                    out.flush();
                } else if(command.get(0).equalsIgnoreCase("WAIT")){
                    // write integer 0 to out
                    int timeout = Integer.parseInt(command.get(2));

                    for(Socket socket : slaves.keySet()){
                        this.slaves.get(socket).getOutputStream().write(("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n").getBytes());
                        this.slaves.get(socket).getOutputStream().flush();

                        try{
                            BufferedReader slaveReader = this.slaves.get(socket).getReader();
                            String line = slaveReader.readLine();
                            System.out.println("Slave response: " + line);
                            slaveReader = this.slaves.get(socket).getReader();
                            line = slaveReader.readLine();
                            System.out.println("Slave response: " + line);
                            slaveReader = this.slaves.get(socket).getReader();
                            line = slaveReader.readLine();
                            System.out.println("Slave response: " + line);
                            slaveReader = this.slaves.get(socket).getReader();
                            line = slaveReader.readLine();
                            System.out.println("Slave response: " + line);
                            slaveReader = this.slaves.get(socket).getReader();
                            line = slaveReader.readLine();
                            System.out.println("Slave response: " + line);
                            slaveReader = this.slaves.get(socket).getReader();
                            line = slaveReader.readLine();
                            System.out.println("Slave response: " + line);
                            slaveReader = this.slaves.get(socket).getReader();
                            line = slaveReader.readLine();
                            System.out.println("Slave response: " + line);
                        }catch (Exception e){
                            System.out.println("Exception while getting response *: " + e.getMessage());
                        }
                    }

                    int replicasReplied = 0;

                    long startTime = System.currentTimeMillis();
                    while((System.currentTimeMillis() - startTime) < timeout || replicasReplied < Integer.parseInt(command.get(1))){
                        for(Socket socket : slaves.keySet()){
                            try{
                                BufferedReader slaveReader = this.slaves.get(socket).getReader();
                                String line = slaveReader.readLine();
                                System.out.println("Slave response: " + line);
                                if(line != null && line.contains("OK")){
                                    replicasReplied++;
                                }
                            }catch (Exception e){
                                System.out.println("Exception while waiting for replicas: " + e.getMessage());
                            }
                        }
                    }
                    System.out.println("Replicas replied: " + replicasReplied);
                }else {
                    if(this.clientType == ClientType.NONDBCLIENT || (this.clientType == ClientType.DBCLIENT && command.get(0).equalsIgnoreCase("REPLCONF"))) this.commandHandler.handleCommand(command, out);
                    else this.commandHandler.handleCommand(command, new OutputStream() {
                        @Override
                        public void write(int b) throws IOException {}
                    });
                }

                if(command.get(0).equalsIgnoreCase("PSYNC") || command.get(0).equalsIgnoreCase("SYNC")){
                    this.slaves.put(clientSocket, new SlaveDetails(1, reader, out));
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

        List<String> command = new java.util.ArrayList<>();
        if (line != null && line.startsWith("*")) {
            int n = Integer.parseInt(line.substring(1));
            for (int i = 0; i < n; i++) {
                line = reader.readLine();
                line = reader.readLine();
                command.add(line);
            }
        }
        // if line starts with $x then fetch next x bytes
        else if (line != null && line.startsWith("$")) {
            int n = Integer.parseInt(line.substring(1));
            char[] buf = new char[n];
            reader.read(buf, 0, n);
            String parsedline = new String(buf);

            line = reader.readLine(); // read the trailing \r\n
            if(parsedline.equalsIgnoreCase("REPLCONF")){
                // hardcoding logic to fetch [REPLCONF, GETACK, *]
                command.add(parsedline);
                line = reader.readLine();
                n = Integer.parseInt(line.substring(1));
                buf = new char[n];
                reader.read(buf, 0, n);
                reader.readLine(); // read the trailing \r\n
                parsedline = new String(buf);
                command.add(parsedline);
                line = reader.readLine();
                n = Integer.parseInt(line.substring(1));
                buf = new char[n];
                reader.read(buf, 0, n);
                reader.readLine(); // read the trailing \r\n
                parsedline = new String(buf);
                command.add(parsedline);
            }else if(parsedline.equalsIgnoreCase("SET")){
                command.add(parsedline);
                line = reader.readLine();
                n = Integer.parseInt(line.substring(1));
                buf = new char[n];
                reader.read(buf, 0, n);
                reader.readLine(); // read the trailing \r\n
                parsedline = new String(buf);
                command.add(parsedline);
                line = reader.readLine();
                n = Integer.parseInt(line.substring(1));
                buf = new char[n];
                reader.read(buf, 0, n);
                reader.readLine(); // read the trailing \r\n
                parsedline = new String(buf);
                command.add(parsedline);
            }
        }
        return command;
    }
}

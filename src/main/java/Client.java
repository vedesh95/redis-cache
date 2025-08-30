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

            List<List<String>> lastcommands = new ArrayList<>();
            while(true){
                List<String> command = parseCommand(reader);
                if(command.isEmpty()) continue;
                lastcommands.add(command);
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
                } else if(command.get(0).equalsIgnoreCase("REPLCONF")){
                    // return number of bytes of commands processed in lastcommands list;
                    // response lookes like
                    // *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$3\r\n154\r\n
                    int totalBytes = 0;
                    for(List<String> cmd : lastcommands){
                        StringBuilder sb = new StringBuilder();
                        sb.append("*").append(cmd.size()).append("\r\n");
                        for(String arg : cmd){
                            sb.append("$").append(arg.length()).append("\r\n");
                            sb.append(arg).append("\r\n");
                        }
                        totalBytes += sb.toString().getBytes().length;
                    }
                    lastcommands.clear();
                    out.write(("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + totalBytes + "\r\n" + totalBytes + "\r\n").getBytes());
                    out.flush();


                    //SET, raspberry, pineapple
                    // resp encoded = *3\r\n$3\r\nSET\r\n$9\r\nraspberry\r\n$9\r\npineapple\r\n = 29 bytes
                    // SET, banana, orange
                    // resp encoded = *3\r\n$3\r\nSET\r\n$6\r\nbanana\r\n$6\r\norange\r\n = 27 bytes
                    // REPLCONF, GETACK, *
                    // resp encoded = *3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n0\r\n = 35 bytes
                    // total = 91 bytes

                    // set foo 1
                    // resp encoded = *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$1\r\n1\r\n = 29 bytes
                    // 4+4+5+4+5+4+4 = 29 bytes
                    // did you understand why 29 bytes are counted?
                    //
                    /*  based on below lines
                        *3\r\n
                        $3\r\n
                        SET\r\n
                        $3\r\n
                        foo\r\n
                        $1\r\n
                        1\r\n
                    */
                    // why 29 bytes are counted till now instead of 25 bytes?
                    //

                }else {
                    if(lastcommands.isEmpty()) this.commandHandler.handleCommand(command, out);
                    else this.commandHandler.handleCommand(command, new OutputStream() {
                        @Override
                        public void write(int b) throws IOException {

                        }
                    });
                }

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
}

import command.Command;
import struct.KeyValue;
import struct.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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

    public Client(CommandHandler commandHandler, Socket clientSocket, ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap){
        this.commandHandler = commandHandler;
        this.clientSocket = clientSocket;
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
        this.transaction = new ArrayList<>();
    }

    public void listen() {
        try (clientSocket; BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())); OutputStream out = clientSocket.getOutputStream()) {
            boolean isInTransaction = false;
            boolean propogateToSlaves = false;
            while(true){
                List<String> command = parseCommand(reader);
                if(command.isEmpty()) continue;
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

                System.out.println("Received command: " + command);
                // if command is PSYNC or SYNC, set propogateToSlaves to true
                if(command.get(0).equalsIgnoreCase("PSYNC") || command.get(0).equalsIgnoreCase("SYNC")){
                    propogateToSlaves = true;
                }
                if(propogateToSlaves && !command.get(0).equalsIgnoreCase("PSYNC") && !command.get(0).equalsIgnoreCase("SYNC") && !command.get(0).equalsIgnoreCase("REPLCONF") && !command.get(0).equalsIgnoreCase("INFO")){
                    System.out.println("Propogating command to slaves: " + command);
                    this.commandHandler.propagateToSlaves(command, out);
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

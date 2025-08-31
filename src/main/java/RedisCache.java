import struct.*;

import java.io.BufferedReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisCache {
    private CommandHandler commandHandler;
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap = new ConcurrentHashMap<>();
    private ServerInfo info;
    private Map<Socket, SlaveDetails> slaves;
    private AtomicInteger ackCounter;

    public RedisCache(){
        this.map = new ConcurrentHashMap<>();
        this.lists = new ConcurrentHashMap<>();
        this.threadsWaitingForBLPOP = new ConcurrentHashMap<>();
        this.streamMap = new ConcurrentHashMap<>();
        this.info = new ServerInfo();
        this.slaves = new ConcurrentHashMap<>();
        ackCounter = new AtomicInteger(0);
        this.commandHandler = new CommandHandler(map, lists, threadsWaitingForBLPOP, streamMap, info, ackCounter);
    }

    public void addClient(Socket clientSocket, ClientType clientType, BufferedReader reader, OutputStream out){
        new Thread(() -> {
            Client client = new Client(commandHandler, clientType, clientSocket, map, lists, threadsWaitingForBLPOP, streamMap, slaves, ackCounter);
            client.listen(clientSocket, reader, out);
        }).start();
    }

    public ServerInfo getInfo() {
        return info;
    }

    public void setInfo(ServerInfo info) {
        this.info = info;
    }
}

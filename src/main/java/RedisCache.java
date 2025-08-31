import struct.*;

import java.io.BufferedReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RedisCache {
    private CommandHandler commandHandler;
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap = new ConcurrentHashMap<>();
    private ServerInfo info;
    private List<Socket> slaves;

    public RedisCache(){
        this.map = new ConcurrentHashMap<>();
        this.lists = new ConcurrentHashMap<>();
        this.threadsWaitingForBLPOP = new ConcurrentHashMap<>();
        this.streamMap = new ConcurrentHashMap<>();
        this.info = new ServerInfo();
        this.commandHandler = new CommandHandler(map, lists, threadsWaitingForBLPOP, streamMap, info);
        this.slaves = new ArrayList<>();
    }

    public void addClient(Socket clientSocket, ClientType clientType, BufferedReader reader, OutputStream out){
        new Thread(() -> {
            Client client = new Client(commandHandler, clientType, clientSocket, map, lists, threadsWaitingForBLPOP, streamMap, slaves);
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

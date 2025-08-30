import struct.ServerInfo;
import struct.KeyValue;
import struct.Pair;

import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RedisCache {
    private CommandHandler commandHandler;
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap = new ConcurrentHashMap<>();
    private ServerInfo info;
    private Map<Socket, Integer> slaves;

    public RedisCache(){
        this.map = new ConcurrentHashMap<>();
        this.lists = new ConcurrentHashMap<>();
        this.threadsWaitingForBLPOP = new ConcurrentHashMap<>();
        this.streamMap = new ConcurrentHashMap<>();
        this.info = new ServerInfo();
        this.commandHandler = new CommandHandler(map, lists, threadsWaitingForBLPOP, streamMap, info);
        this.slaves = new HashMap<>();
    }

    public void addClient(Socket clientSocket){
        Client client = new Client(commandHandler, clientSocket, map, lists, threadsWaitingForBLPOP, streamMap, slaves);
        new Thread(client::listen).start();
    }

    public ServerInfo getInfo() {
        return info;
    }

    public void setInfo(ServerInfo info) {
        this.info = info;
    }
}

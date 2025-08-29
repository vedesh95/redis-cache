import struct.ServerInfo;
import struct.KeyValue;
import struct.Pair;

import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Cache {
    private CommandHandler commandHandler;
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap = new ConcurrentHashMap<>();
    private ServerInfo info;

    public Cache(){
        this.map = new ConcurrentHashMap<>();
        this.lists = new ConcurrentHashMap<>();
        this.threadsWaitingForBLPOP = new ConcurrentHashMap<>();
        this.streamMap = new ConcurrentHashMap<>();
        this.info = new ServerInfo();

        commandHandler = new CommandHandler(map, lists, threadsWaitingForBLPOP, streamMap, info);
    }

    public void addClient(Socket clientSocket){
        Client client = new Client(commandHandler, clientSocket, map, lists, threadsWaitingForBLPOP, streamMap);
        new Thread(client::listen).start();
    }

    public ServerInfo getInfo() {
        return info;
    }

    public void setInfo(ServerInfo info) {
        this.info = info;
    }
}

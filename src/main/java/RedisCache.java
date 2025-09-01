import rdbparser.RDBParser;
import struct.*;
import struct.SortedSet;

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
    private RDBDetails rdbDetails;
    private RDBParser rdbparser;
    private Map<String, Set<Socket> > pubSubMap;
    private Map<Socket, Set<String> > subPubMap;
    private SortedSet sortedSet;

    public RedisCache(){
        this.map = new ConcurrentHashMap<>();
        this.lists = new ConcurrentHashMap<>();
        this.threadsWaitingForBLPOP = new ConcurrentHashMap<>();
        this.streamMap = new ConcurrentHashMap<>();
        this.info = new ServerInfo();
        this.slaves = new ConcurrentHashMap<>();
        this.ackCounter = new AtomicInteger(0);
        this.rdbDetails = new RDBDetails();
        this.rdbparser = new RDBParser();
        this.pubSubMap = new ConcurrentHashMap<>();
        this.subPubMap = new ConcurrentHashMap<>();
        this.sortedSet  = new SortedSet();
        this.commandHandler = new CommandHandler(map, lists, threadsWaitingForBLPOP, streamMap, info, ackCounter, rdbDetails, rdbparser, pubSubMap, subPubMap, sortedSet);
    }

    public void addClient(Socket clientSocket, ClientType clientType, BufferedReader reader, OutputStream out){
        new Thread(() -> {
            Client client = new Client(commandHandler, clientType, clientSocket, map, lists, threadsWaitingForBLPOP, streamMap, slaves, ackCounter, rdbDetails, rdbparser, pubSubMap, subPubMap, sortedSet);
            client.listen(clientSocket, reader, out);
        }).start();
    }

    public ServerInfo getInfo() {
        return info;
    }

    public void setInfo(ServerInfo info) {
        this.info = info;
    }

    public void initRDBParser() {
        if(this.rdbDetails.getDir() != null && this.rdbDetails.getDbfilename() != null) {
            try {
                this.rdbparser.parse(this.rdbDetails.getDir() + "/" + this.rdbDetails.getDbfilename());
            } catch (Exception e) {
                System.out.println("Error parsing RDB file: " + e.getMessage());
            }
        }
    }

    public RDBDetails getRdbDetails() {
        return rdbDetails;
    }

    public void setRdbDetails(RDBDetails rdbDetails) {
        this.rdbDetails = rdbDetails;
    }

    public RDBParser getRdbparser() {
        return rdbparser;
    }

    public void setRdbparser(RDBParser rdbparser) {
        this.rdbparser = rdbparser;
    }
}

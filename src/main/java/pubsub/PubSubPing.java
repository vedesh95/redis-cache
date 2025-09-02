package pubsub;

import rdbparser.RDBParser;
import struct.KeyValue;
import struct.Pair;
import struct.RDBDetails;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PubSubPing implements PubSubCommand {
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue>>> streamMap = new ConcurrentHashMap<>();
    private RDBDetails rdbDetails;
    private RDBParser rdbparser;
    private Map<String, Set<Socket>> pubSubMap;
    private Map<Socket, Set<String>> subPubMap;

    public PubSubPing(ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue>>> streamMap, RDBDetails rdbDetails, RDBParser rdbparser, Map<String, Set<Socket>> pubSubMap, Map<Socket, Set<String>> subPubMap) {
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
        this.rdbDetails = rdbDetails;
        this.rdbparser = rdbparser;
        this.pubSubMap = pubSubMap;
        this.subPubMap = subPubMap;
    }

    @Override
    public void execute(List<String> command, OutputStream out, Socket socket) throws IOException {
        // resp array of pong and empty string
        out.write("*2\r\n".getBytes());
        out.write("$4\r\npong\r\n".getBytes());
        out.write("$0\r\n\r\n".getBytes());
        out.flush();
    }
}
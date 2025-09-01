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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Subscribe implements PubSubCommand{
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap = new ConcurrentHashMap<>();
    private RDBDetails rdbDetails;
    private RDBParser rdbparser;
    private Map<String, Socket> pubSubMap;
    private Map<Socket, String> subPubMap;

    public Subscribe(ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap, RDBDetails rdbDetails, RDBParser rdbparser, Map<String, Socket> pubSubMap, Map<Socket, String> subPubMap){
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
        String channel = command.get(1);
        this.pubSubMap.put(channel, socket);
        this.subPubMap.put(socket, channel);

        out.write("*3\r\n".getBytes());
        out.write("$9\r\nsubscribe\r\n".getBytes());
        out.write(("$" + channel.length() + "\r\n" + channel + "\r\n").getBytes());
        out.write(("$" + subPubMap.get(socket).length() + "\r\n" + subPubMap.get(socket) + "\r\n").getBytes());
        out.flush();
    }

}

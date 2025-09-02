package command;

import rdbparser.RDBParser;
import struct.KeyValue;
import struct.Pair;
import struct.RDBDetails;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Keys implements Command{

    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap = new ConcurrentHashMap<>();
    private RDBDetails rdbDetails;
    private RDBParser rdbparser;

    public Keys(ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap, RDBDetails rdbDetails, RDBParser rdbparser){
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
        this.rdbDetails = rdbDetails;
        this.rdbparser = rdbparser;
    }

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        if(command.get(1).equalsIgnoreCase("*")){
            // collect all keys from entries. each element in the entries is a object of type class RedisEntry containing field key
            List<String> keys = new ArrayList<>();
            for(RDBParser.RedisEntry entry : rdbparser.entries) keys.add(entry.key);
            out.write(("*" + keys.size() + "\r\n").getBytes());
            for(String key : keys){
                out.write(("$" + key.length() + "\r\n" + key + "\r\n").getBytes());
                out.flush();
            }
        }
    }
}

package command;

import rdbparser.RDBParser;
import struct.KeyValue;
import struct.Pair;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

public class Get implements Command{

    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap = new ConcurrentHashMap<>();
    private RDBParser rdbparser;

    public Get(ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap, RDBParser rdbparser){
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
        this.rdbparser = rdbparser;
    }

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        String key = command.get(1);
        String value = null;
        if(map.containsKey(key) && (map.get(key).expireTime == null || map.get(key).expireTime + map.get(key).time.getTime() > System.currentTimeMillis())){
            value = map.get(key).value;
        }
        if(value == null){
            for(RDBParser.RedisEntry entry : rdbparser.entries){
                if(entry.key.equals(key) && entry.expiry < System.currentTimeMillis()){
                    value = entry.value;
                }
            }
        }
        if(value!=null){
            out.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
            out.flush();
        }else{
            out.write("$-1\r\n".getBytes());
            out.flush();
        }
    }
}

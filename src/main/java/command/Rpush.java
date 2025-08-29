package command;

import struct.KeyValue;
import struct.Pair;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Rpush implements Command{

    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap = new ConcurrentHashMap<>();

    public Rpush(ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap){
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
    }

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        String key = command.get(1);
        for(int i = 2; i < command.size(); i++){
            String value = command.get(i);
            if(!lists.containsKey(key)){
                lists.put(key, new ArrayList<>());
            }
            lists.get(key).add(value);
        }
        out.write((":" + lists.get(key).size() + "\r\n").getBytes());
        out.flush();
    }
}

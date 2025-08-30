package command;

import struct.KeyValue;
import struct.Pair;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Set implements Command{

    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap = new ConcurrentHashMap<>();

    public Set(ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap){
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
    }

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        System.out.println("SET command: " + command);
        if(command.size()==5){
            String key = command.get(1);
            String value = command.get(2);
            this.map.put(key, new Pair(value, Integer.valueOf(command.get(4))));
            out.write("+OK\r\n".getBytes());
            out.flush();
        } else {
            String key = command.get(1);
            String value = command.get(2);
            this.map.put(key, new Pair(value, null));
            System.out.println("SET " + key + " " + value);
            out.write("+OK\r\n".getBytes());
            out.flush();
        }
    }
}

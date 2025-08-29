package command;

import struct.KeyValue;
import struct.Pair;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Blpop implements Command {
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue>>> streamMap = new ConcurrentHashMap<>();

    public Blpop(ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue>>> streamMap) {
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
    }

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        String key = command.get(1);
        double timeout = Double.parseDouble(command.get(2)) * 1000; // convert to milliseconds
        boolean waitForever = timeout == 0;
        if (!threadsWaitingForBLPOP.containsKey(key)) {
            threadsWaitingForBLPOP.put(key, new ConcurrentLinkedQueue<>());
        }
        threadsWaitingForBLPOP.get(key).offer(Thread.currentThread());
        long startTime = System.currentTimeMillis();
        boolean found = false;
        while (waitForever || (System.currentTimeMillis() - startTime) < timeout) {
            if(threadsWaitingForBLPOP.get(key).peek() == Thread.currentThread()){
                if (lists.containsKey(key) && !lists.get(key).isEmpty()) {
                    String value = lists.get(key).remove(0);
                    out.write(("*2\r\n$" + key.length() + "\r\n" + key + "\r\n" + "$" + value.length() + "\r\n" + value + "\r\n").getBytes());
                    out.flush();
                    threadsWaitingForBLPOP.get(key).remove(Thread.currentThread());
                    found = true;
                    break;
                }
            }
        }

        if(!found){
            out.write("*-1\r\n".getBytes());
            out.flush();
            threadsWaitingForBLPOP.get(key).remove(Thread.currentThread());
        }
    }
}

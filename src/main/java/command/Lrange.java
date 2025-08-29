package command;

import struct.KeyValue;
import struct.Pair;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Lrange implements Command{
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap = new ConcurrentHashMap<>();

    public Lrange(ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap){
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
    }

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        String key = command.get(1);
        int start = Integer.parseInt(command.get(2));
        int end = Integer.parseInt(command.get(3));
        if(!lists.containsKey(key)){
            out.write("*0\r\n".getBytes());
            out.flush();
        } else {
            List<String> list = lists.get(key);
            if(end >= list.size()) end = list.size() - 1;
            if(start < 0) start = list.size() + start;
            if(end< 0) end = list.size() + end;
            if(start < 0) start = 0; //suppose list is of size 6 and start is -7
            if(start > end || start >= list.size()){
                out.write("*0\r\n".getBytes());
                out.flush();
            } else {
                out.write(("*" + (end - start + 1) + "\r\n").getBytes());
                for(int i = start; i <= end; i++){
                    String value = list.get(i);
                    out.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
                }
                out.flush();
            }
        }
    }
}

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

public class Lpop implements Command{
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap = new ConcurrentHashMap<>();

    public Lpop(ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap){
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
    }

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        String key = command.get(1);
        if(!lists.containsKey(key) || lists.get(key).isEmpty()){
            out.write("$-1\r\n".getBytes());
            out.flush();
        } else {
            int count = 1;
            if(command.size() == 3){
                count = Integer.parseInt(command.get(2));
            }
            // if list not found or empty
            if(!lists.containsKey(key) || lists.get(key).isEmpty()){
                out.write("$-1\r\n".getBytes());
                out.flush();
                return;
            }

            List<String> list = lists.get(key);
            if(count > list.size()){
                out.write(("*" + list.size() + "\r\n").getBytes());
                for(String value : list){
                    out.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
                }
                lists.put(key, Collections.synchronizedList(new ArrayList<>()));
                out.flush();
            } else {
                if(count == 1){
                    String value = list.remove(0);
                    out.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
                    out.flush();
                    return;
                }
                out.write(("*" + count + "\r\n").getBytes());
                for(int i = 0; i < count; i++){
                    String value = list.remove(0);
                    out.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
                }
                out.flush();
            }
        }
    }
}

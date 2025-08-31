package command;

import struct.KeyValue;
import struct.Pair;
import struct.RDBDetails;

import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Config implements Command{
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap = new ConcurrentHashMap<>();
    private RDBDetails rdbDetails;

    public Config(ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap, RDBDetails rdbDetails){
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
        this.rdbDetails = rdbDetails;
    }

    @Override
    public void execute(List<String> command, OutputStream out) throws java.io.IOException {
        String line = command.get(1);
        if(command.get(1).equalsIgnoreCase("GET")){
            String param = command.get(2);
            // return parameter value, encoded as a RESP Bulk string
            if(param.equalsIgnoreCase("dbfilename")){
                // return resp array of param name and value
                out.write("*2\r\n".getBytes());
                out.write(("$" + param.length() + "\r\n" + param + "\r\n").getBytes());
                out.write(("$" + rdbDetails.getDbfilename().length() + "\r\n" + rdbDetails.getDbfilename() + "\r\n").getBytes());
                out.flush();

            } else if(param.equalsIgnoreCase("dir")){
                out.write("*2\r\n".getBytes());
                out.write(("$" + param.length() + "\r\n" + param + "\r\n").getBytes());
                out.write(("$" + rdbDetails.getDir().length() + "\r\n" + rdbDetails.getDir() + "\r\n").getBytes());
                out.flush();
            } else {
                out.write("$-1\r\n".getBytes());
                out.flush();
            }
        }
    }
}

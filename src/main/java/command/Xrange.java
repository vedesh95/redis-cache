package command;

import struct.KeyValue;
import struct.Pair;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Xrange implements Command {
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue>>> streamMap = new ConcurrentHashMap<>();

    public Xrange(ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue>>> streamMap) {
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
    }

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        String streamid = command.get(1);
        String startid = command.get(2);
        String endid = command.get(3);
        if (!streamMap.containsKey(streamid)) {
            out.write("*0\r\n".getBytes());
            out.flush();
            return;
        }
        // The sequence number doesn't need to be included in the start and end IDs provided to the command. If not provided, XRANGE defaults to a sequence number of 0 for the start and the maximum sequence number for the end.
        // start can also be specified as -
        // end can also be specified as +
        if (startid.equals("-")) startid = "0-0";
        if (endid.equals("+")) endid = Integer.MAX_VALUE + "-" + Integer.MAX_VALUE;

        String[] startIdParts = startid.split("-");
        String[] endIdParts = endid.split("-");
        if (startIdParts.length == 1) {
            startid = startIdParts[0] + "-0";
            startIdParts = startid.split("-");
        }
        if (endIdParts.length == 1) {
            endid = endIdParts[0] + "-" + Integer.MAX_VALUE;
            endIdParts = endid.split("-");
        }
        List<String> result = new ArrayList<>();
        for (String entryId : streamMap.get(streamid).keySet()) {
            String[] entryIdParts = entryId.split("-");
            if ((Integer.parseInt(entryIdParts[0]) > Integer.parseInt(startIdParts[0]) ||
                    (Integer.parseInt(entryIdParts[0]) == Integer.parseInt(startIdParts[0]) &&
                            Integer.parseInt(entryIdParts[1]) >= Integer.parseInt(startIdParts[1])))
                    &&
                    (Integer.parseInt(entryIdParts[0]) < Integer.parseInt(endIdParts[0]) ||
                            (Integer.parseInt(entryIdParts[0]) == Integer.parseInt(endIdParts[0]) &&
                                    Integer.parseInt(entryIdParts[1]) <= Integer.parseInt(endIdParts[1])))) {
                result.add(entryId);
            }
        }

        // generate RESP array
        out.write(("*" + result.size() + "\r\n").getBytes());
        for (String entryId : result) {
            List<KeyValue> keyValues = streamMap.get(streamid).get(entryId);
            out.write(("*2\r\n$" + entryId.length() + "\r\n" + entryId + "\r\n" + "*" + (keyValues.size() * 2) + "\r\n").getBytes());
            for (KeyValue kv : keyValues) {
                out.write(("$" + kv.key.length() + "\r\n" + kv.key + "\r\n").getBytes());
                out.write(("$" + kv.value.length() + "\r\n" + kv.value + "\r\n").getBytes());
            }
        }
    }
}
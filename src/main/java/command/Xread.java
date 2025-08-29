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

public class Xread implements Command {
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue>>> streamMap = new ConcurrentHashMap<>();

    public Xread(ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue>>> streamMap) {
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
    }

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        long startTime = System.currentTimeMillis();
        int count = Integer.MAX_VALUE;
        int index = 1;

        long timeout = Long.MAX_VALUE;
        boolean blocking = false;

        if(command.get(index).equalsIgnoreCase("BLOCK")) {
            timeout = Long.parseLong(command.get(index + 1));
            if(timeout==0) timeout = Long.MAX_VALUE; // wait forever
            index += 2;
            blocking = true;
        }else if(command.get(index).equalsIgnoreCase("COUNT")){
            count = Integer.parseInt(command.get(index+1));
            index += 2;
        }

        boolean fetchNew = false;
        if(command.get(command.size()-1).equals("$")){
            command.remove(command.size()-1);
            fetchNew = true;
        }

        if(!command.get(index).equalsIgnoreCase("STREAMS")){
            out.write("-ERR syntax error\r\n".getBytes());
            out.flush();
            return;
        }

        index++;
        List<String> streamids = new ArrayList<>();
        List<String> entryids = new ArrayList<>();
        List<List<String>> results = new ArrayList<>();
        while(index < command.size()){
            streamids.add(command.get(index));
            index++;
        }

        if(fetchNew) {
            int n = streamids.size();
            // entry id should be greater than the last entry id in the stream
            for (int i = 0; i < n; i++) {
                String streamid = streamids.get(i);
                if (!streamMap.containsKey(streamid) || streamMap.get(streamid).isEmpty()) {
                    streamids.add("0-0");
                } else {
                    String lastEntryId = null;
                    for (String key : streamMap.get(streamid).keySet()) {
                        lastEntryId = key;
                    }
                    String[] lastEntryIdParts = lastEntryId.split("-");
                    String entryid = lastEntryIdParts[0] + "-" + Integer.parseInt(lastEntryIdParts[1]);
                    streamids.add(entryid);
                }
            }
            System.out.println("XREAD fetchNew: streamids=" + streamids);
        }


        int mid = streamids.size()/2;
        entryids = streamids.subList(mid, streamids.size());
        streamids = streamids.subList(0, mid);
        if(streamids.size() != entryids.size()){
            out.write("-ERR syntax error\r\n".getBytes());
            out.flush();
            return;
        }
        do{
            // command for xread goes something like [XREAD, streams, stream-1, stream-2, range-1, range-2
            for(int i = 0; i < streamids.size(); i++) {
                String streamid = streamids.get(i);
                String entryid = entryids.get(i);
                if (!streamMap.containsKey(streamid)) {
                    results.add(new ArrayList<>());
                    out.write(("*" + results.size() + "\r\n").getBytes());
                    continue;
                }
                if (entryid.equals("-")) entryid = "0-0";
                if (entryid.equals("+")) entryid = Integer.MAX_VALUE + "-" + Integer.MAX_VALUE;

                String[] entryIdParts = entryid.split("-");
                if (entryIdParts.length == 1) {
                    entryid = entryIdParts[0] + "-0";
                    entryIdParts = entryid.split("-");
                }

                // XREAD returns an array where each element is an array composed of two elements, which are the ID and the list of fields and values.
                List<String> result = new ArrayList<>();
                int c = 0;

                for (String eid : streamMap.get(streamid).keySet()) {
                    String[] eidParts = eid.split("-");
                    if ((Integer.parseInt(eidParts[0]) > Integer.parseInt(entryIdParts[0]) ||
                            (Integer.parseInt(eidParts[0]) == Integer.parseInt(entryIdParts[0]) &&
                                    Integer.parseInt(eidParts[1]) > Integer.parseInt(entryIdParts[1])))) {
                        result.add(eid);
                        System.out.println("XREAD: streamid=" + streamid + ", entryid=" + entryid + ", found eid=" + eid);
                        c++;
                        if (c >= count || blocking) break;
                    }
                }

                // write RESP array for this stream
                if(result.size()!=0) results.add(result);
            }
        }while(blocking && results.size()==0 && (System.currentTimeMillis() - startTime) < timeout);

        // if no results found after timeout
        System.out.println("results=" + results);
        if(results.isEmpty()){
            System.out.println("got empty results");
            out.write("$-1\r\n".getBytes());
            out.flush();
            return;
        }
        out.write(("*" + results.size() + "\r\n").getBytes());
        for(int i = 0; i < results.size(); i++) {
            String streamid = streamids.get(i);
            List<String> result = results.get(i);
            out.write(("*2\r\n$" + streamid.length() + "\r\n" + streamid + "\r\n" + "*" + result.size() + "\r\n").getBytes());
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
}

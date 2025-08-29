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

public class Xadd implements Command{
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap = new ConcurrentHashMap<>();

    public Xadd(ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap){
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
    }

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        String streamid = command.get(1);
        String entryid = getEntryId(command, out);
        if(entryid.equals("-1")) return; // error already handled in getEntryId
        if(!streamMap.get(streamid).containsKey(entryid)){
            streamMap.get(streamid).put(entryid, new ArrayList<>());
        }

        String id = command.get(2);
        String key = command.get(3);
        String value = command.get(4);
        streamMap.get(streamid).get(entryid).add(new KeyValue(null, key, value));

        out.write(("$" + entryid.length() + "\r\n" + entryid + "\r\n").getBytes());
        out.flush();
    }

    public String getEntryId(List<String> command, OutputStream out) throws IOException {
        // The entryid should be greater than the ID of the last entry in the stream.
        // The millisecondsTime part of the ID should be greater than or equal to the millisecondsTime of the last entry.
        // If the millisecondsTime part of the ID is equal to the millisecondsTime of the last entry, the sequenceNumber part of the ID should be greater than the sequenceNumber of the last entry.
        // When * is used with the XADD command, Redis auto-generates a unique auto-incrementing ID for the message being appended to the stream.
        // Redis defaults to using the current unix time in milliseconds for the time part and 0 for the sequence number. If the time already exists in the stream, the sequence number for that record incremented by one will be used.

        String streamid = command.get(1);
        String entryid = command.get(2);
        if (!streamMap.containsKey(streamid)) {
            streamMap.put(streamid, new LinkedHashMap<>());
        }

        String lastEntryId = null;
        for (String key : streamMap.get(streamid).keySet()) {
            lastEntryId = key;
        }
        String[] entryIdParts = entryid.split("-");
        String[] lastEntryIdParts = {};
        if (lastEntryId != null) lastEntryIdParts = lastEntryId.split("-");

        if (entryIdParts.length == 1 && entryIdParts[0].equals("*")) {
            long currentTimeMillis = System.currentTimeMillis();
            if (lastEntryIdParts.length > 0 && Long.parseLong(lastEntryIdParts[0]) == currentTimeMillis) {
                entryid = lastEntryIdParts[0] + "-" + (Integer.parseInt(lastEntryIdParts[1]) + 1);
            } else {
                entryid = currentTimeMillis + "-0";
            }
            return entryid;
        }

        if (entryIdParts[1].equals("*")) {
            if (lastEntryIdParts.length > 0 && entryIdParts[0].equals(lastEntryIdParts[0])) {
                entryid = entryIdParts[0] + "-" + (Integer.parseInt(lastEntryIdParts[1]) + 1);
            } else if (entryIdParts[0].equals("0")) {
                entryid = entryIdParts[0] + "-1";
            } else {
                entryid = entryIdParts[0] + "-0";
            }
            return entryid;
        }

        // The minimum entry ID that Redis supports is 0-1
        if (Integer.parseInt(entryIdParts[0]) <= 0 && Integer.parseInt(entryIdParts[1]) <= 0) {
            try {
                out.write("-ERR The ID specified in XADD must be greater than 0-0\r\n".getBytes());
                out.flush();
            } catch (IOException e) {
                System.out.println(e);
            }
            return "-1";
        }
        if (lastEntryId != null && (Integer.parseInt(entryIdParts[0]) < Integer.parseInt(lastEntryIdParts[0]) ||
                (Integer.parseInt(entryIdParts[0]) == Integer.parseInt(lastEntryIdParts[0]) &&
                        Integer.parseInt(entryIdParts[1]) <= Integer.parseInt(lastEntryIdParts[1])))) {
            try {
                out.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".getBytes());
                out.flush();
            } catch (IOException e) {
                System.out.println(e);
            }
            return "-1";
        }
        return entryid;
    }
}


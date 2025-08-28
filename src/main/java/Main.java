import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Time;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

class Pair{
    public String value;
    public Time time;
    public Integer expireTime;
    public Pair(String value, Integer expireTime) {
        this.value = value;
        time = new Time(System.currentTimeMillis());
        this.expireTime = expireTime;
    }
}

class KeyValue{
    public String id;
    public String key;
    public String value;
    public KeyValue(String id, String key, String value) {
        this.id = id;
        this.key = key;
        this.value = value;
    }
}

@SuppressWarnings("InfiniteLoopStatement")
public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

//      Uncomment this block to pass the first stage
        ServerSocket serverSocket = null;
//        Socket clientSocket = null;
        int port = 6379;
      ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
      ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
      ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
      ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue> >> streamMap = new ConcurrentHashMap<>();
        try {
          serverSocket = new ServerSocket(port);
          // Since the tester restarts your program quite often, setting SO_REUSEADDR
          // ensures that we don't run into 'Address already in use' errors
          serverSocket.setReuseAddress(true);
          while (true){
            Socket clientSocket = serverSocket.accept();
            spinThread(clientSocket, map, lists, threadsWaitingForBLPOP, streamMap);
          }
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        }
  }

    public static void spinThread(Socket clientSocket, ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue>>>  streamMap){
        new Thread(() -> {
            try (clientSocket;
                 BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 OutputStream out = clientSocket.getOutputStream()) {


                while(true){
                    List<String> command = parseCommand(reader);
                    if(command.isEmpty()) continue;

                    if (command.get(0).equalsIgnoreCase("PING")) {
                        out.write("+PONG\r\n".getBytes());
                        out.flush();
                    } else if (command.get(0).startsWith("ECHO")) {
                        String line = command.get(1);
//                        out.write((command.get(0) + "\r\n" + line + "\r\n").getBytes());
                        out.write(("$" + line.length() + "\r\n" + line + "\r\n").getBytes());

                        out.flush();
                    } else if(command.get(0).contains("SET")){

                        if(command.size()==5){
                            String key = command.get(1);
                            String value = command.get(2);
                            map.put(key, new Pair(value, Integer.valueOf(command.get(4))));
                            out.write("+OK\r\n".getBytes());
                            out.flush();
                        } else {
                            String key = command.get(1);
                            String value = command.get(2);
                            map.put(key, new Pair(value, null));
                            out.write("+OK\r\n".getBytes());
                            out.flush();
                        }
                    } else if(command.get(0).contains("GET")){

                        String key = command.get(1);
                        if(map.containsKey(key) && (map.get(key).expireTime == null || map.get(key).expireTime + map.get(key).time.getTime() > System.currentTimeMillis())){
                            String value = map.get(key).value;
                            out.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
                            out.flush();
                        } else {
                            out.write("$-1\r\n".getBytes());
                            out.flush();
                        }
                    } else if(command.get(0).contains("RPUSH")){
                        // rpush might send multiple values
                        // RPUSH mylist "hello" "world"
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
                    } else if(command.get(0).contains("LRANGE")){
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
                    } else if(command.get(0).contains("LPUSH")){
                        // lpush might send multiple values
                        // LPUSH mylist "hello" "world"
                        String key = command.get(1);
                        for(int i = 2; i < command.size(); i++){
                            String value = command.get(i);
                            if(!lists.containsKey(key)){
                                lists.put(key, new ArrayList<>());
                            }
                            lists.get(key).add(0, value);
                        }
                        out.write((":" + lists.get(key).size() + "\r\n").getBytes());
                        out.flush();
                    } else if(command.get(0).contains("LLEN")){
                        String key = command.get(1);
                        if(!lists.containsKey(key)){
                            out.write(":0\r\n".getBytes());
                            out.flush();
                        } else {
                            out.write((":" + lists.get(key).size() + "\r\n").getBytes());
                            out.flush();
                        }
                    } else if(command.get(0).equals("LPOP")){
                        String key = command.get(1);
                        if(!lists.containsKey(key) || lists.get(key).isEmpty()){
                            out.write("$-1\r\n".getBytes());
                            out.flush();
                        } else {
                            // lpop can ask to remove multiple values
                            // LPOP mylist 2
                            // If the number of elements to remove is greater than the list length, it returns RESP encoded array of all the elements of the list.
                            int count = 1;
                            if(command.size() == 3){
                                count = Integer.parseInt(command.get(2));
                            }
                            // if list not found or empty
                            if(!lists.containsKey(key) || lists.get(key).isEmpty()){
                                out.write("$-1\r\n".getBytes());
                                out.flush();
                                continue;
                            }

                            List<String> list = lists.get(key);
                            if(count > list.size()){
                                out.write(("*" + list.size() + "\r\n").getBytes());
                                for(String value : list){
                                    out.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
                                }
                                lists.put(key, new ArrayList<>());
                                out.flush();
                            } else {
                                // if count == 1 then return a bulk string
                                if(count == 1){
                                    String value = list.remove(0);
                                    out.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
                                    out.flush();
                                    continue;
                                }
                                out.write(("*" + count + "\r\n").getBytes());
                                for(int i = 0; i < count; i++){
                                    String value = list.remove(0);
                                    out.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
                                }
                                out.flush();
                            }

                        }
                    } else if(command.get(0).equals("BLPOP")) {
                        /*
                        If a timeout duration is supplied, it is the number of seconds the client will wait for an element to be available for removal. If no elements were inserted during this interval, the server returns a null bulk string ($-1\r\n).
                        If an element was inserted during this interval, the server removes it from the list and responds to the blocking client with a RESP-encoded array containing two elements:
                        The list name (as a bulk string)
                        The element that was popped (as a bulk string)
                           If multiple clients are blocked for BLPOP command, the server responds to the client which has been blocked for the longest duration.
                        * */

                        String key = command.get(1);
                        Double timeout = Double.parseDouble(command.get(2)) * 1000; // convert to milliseconds
                        boolean waitForever = timeout == 0;
                        Thread currentThread = Thread.currentThread();
                        if (threadsWaitingForBLPOP.containsKey(key) == false) {
                            threadsWaitingForBLPOP.put(key, new ConcurrentLinkedQueue<>());
                        }
                        threadsWaitingForBLPOP.get(key).offer(currentThread);
                        long startTime = System.currentTimeMillis();

                        while (waitForever || (System.currentTimeMillis() - startTime) < timeout) {
                            if(threadsWaitingForBLPOP.get(key).peek() == currentThread){
                                if (lists.containsKey(key) && !lists.get(key).isEmpty()) {
                                    String value = lists.get(key).remove(0);
                                    out.write(("*2\r\n$" + key.length() + "\r\n" + key + "\r\n" + "$" + value.length() + "\r\n" + value + "\r\n").getBytes());
                                    out.flush();
                                    threadsWaitingForBLPOP.get(key).remove(currentThread);
                                    break;
                                }
                            }
                        }
                        // timeout reached or operation completed
                        // if operation completed, thread already removed from queue
                        if(threadsWaitingForBLPOP.get(key).contains(currentThread)){
                            out.write("$-1\r\n".getBytes());
                            out.flush();
                            threadsWaitingForBLPOP.get(key).remove(currentThread);
                        }
                    }else if(command.get(0).equals("TYPE")){
                        String key = command.get(1);
                        if(map.containsKey(key)){
                            out.write("+string\r\n".getBytes());
                            out.flush();
                        } else if (streamMap.containsKey(key)) {
                            out.write("+stream\r\n".getBytes());
                            out.flush();
                        }
                        else {
                            out.write("+none\r\n".getBytes());
                            out.flush();
                        }
                    } else if(command.get(0).contains("XADD")){
                        String streamid = command.get(1);
                        String entryid = getEntryId(command, out, streamMap);
                        if(entryid.equals("-1")) continue; // error already handled in getEntryId
                        if(!streamMap.get(streamid).containsKey(entryid)){
                            streamMap.get(streamid).put(entryid, new ArrayList<>());
                        }

                        String id = command.get(2);
                        String key = command.get(3);
                        String value = command.get(4);
                        streamMap.get(streamid).get(entryid).add(new KeyValue(null, key, value));

                        out.write(("$" + entryid.length() + "\r\n" + entryid + "\r\n").getBytes());
                        out.flush();
                    } else if(command.get(0).contains("XRANGE")){
                        // It takes two arguments: start and end. Both are entry IDs. The command returns all entries in the stream with IDs between the start and end IDs. This range is "inclusive", which means that the response will includes entries with IDs that are equal to the start and end IDs.
                        String streamid = command.get(1);
                        String startid = command.get(2);
                        String endid = command.get(3);
                        if(!streamMap.containsKey(streamid)){
                            out.write("*0\r\n".getBytes());
                            out.flush();
                            continue;
                        }
                        // The sequence number doesn't need to be included in the start and end IDs provided to the command. If not provided, XRANGE defaults to a sequence number of 0 for the start and the maximum sequence number for the end.
                        // start can also be specified as -
                        // end can also be specified as +
                        if(startid.equals("-")) startid = "0-0";
                        if(endid.equals("+")) endid = Integer.MAX_VALUE + "-" + Integer.MAX_VALUE;

                        String[] startIdParts = startid.split("-");
                        String[] endIdParts = endid.split("-");
                        if(startIdParts.length==1){
                            startid = startIdParts[0] + "-0";
                            startIdParts = startid.split("-");
                        }
                        if(endIdParts.length==1){
                            endid = endIdParts[0] + "-"+ Integer.MAX_VALUE;
                            endIdParts = endid.split("-");
                        }
                        List<String> result = new ArrayList<>();
                        for(String entryId : streamMap.get(streamid).keySet()){
                            String[] entryIdParts = entryId.split("-");
                            if((Integer.parseInt(entryIdParts[0]) > Integer.parseInt(startIdParts[0]) ||
                                    (Integer.parseInt(entryIdParts[0]) == Integer.parseInt(startIdParts[0]) &&
                                            Integer.parseInt(entryIdParts[1]) >= Integer.parseInt(startIdParts[1])))
                                    &&
                                    (Integer.parseInt(entryIdParts[0]) < Integer.parseInt(endIdParts[0]) ||
                                            (Integer.parseInt(entryIdParts[0]) == Integer.parseInt(endIdParts[0]) &&
                                                    Integer.parseInt(entryIdParts[1]) <= Integer.parseInt(endIdParts[1])))){
                                result.add(entryId);
                            }
                        }
                        // The actual return value is a RESP Array of arrays.
                        // Each inner array represents an entry.
                        // The first item in the inner array is the ID of the entry.
                        // The second item is a list of key value pairs, where the key value pairs are represented as a list of strings.
                        // The key value pairs are in the order they were added to the entry.

                        // generate RESP array
                        out.write(("*" + result.size() + "\r\n").getBytes());
                        for(String entryId : result) {
                            List<KeyValue> keyValues = streamMap.get(streamid).get(entryId);
                            out.write(("*2\r\n$" + entryId.length() + "\r\n" + entryId + "\r\n" + "*" + (keyValues.size() * 2) + "\r\n").getBytes());
                            for (KeyValue kv : keyValues) {
                                out.write(("$" + kv.key.length() + "\r\n" + kv.key + "\r\n").getBytes());
                                out.write(("$" + kv.value.length() + "\r\n" + kv.value + "\r\n").getBytes());
                            }
                        }
                    } else if(command.get(0).equalsIgnoreCase("XREAD")){
                        // XREAD COUNT 2 STREAMS mystream 0-0
                        // The command returns entries from one or more streams, starting from the specified IDs.
                        // The command takes the following arguments:
                        // COUNT: Optional. The maximum number of entries to return from each stream. If not specified, all available entries are returned.
                        // STREAMS: Required. Indicates that the following arguments are stream names and IDs.
                        // stream1, stream2, ...: The names of the streams to read from.
                        // id1, id2, ...: The IDs to start reading from for each stream. An ID of 0-0 means to read all entries from the beginning of the stream.
                        int count = Integer.MAX_VALUE;
                        int index = 1;
                        // set timeout to highest value by default
                        long timeout = Long.MAX_VALUE;
                        // xread with block implemented here
                        if(command.get(index).equalsIgnoreCase("BLOCK")) {
                            // BLOCK milliseconds
                            timeout = Long.parseLong(command.get(index + 1));
                            index += 2;
                        }else if(command.get(index).equalsIgnoreCase("COUNT")){
                            count = Integer.parseInt(command.get(index+1));
                            index += 2;
                        }

                        if(!command.get(index).equalsIgnoreCase("STREAMS")){
                            out.write("-ERR syntax error\r\n".getBytes());
                            out.flush();
                            continue;
                        }
                        index++;
                        List<String> streamids = new ArrayList<>();
                        List<String> entryids = new ArrayList<>();
                        List<List<String>> results = new ArrayList<>();

                        // implement blocking if needed
                        Long startTime = System.currentTimeMillis();
                        while(index < command.size()){
                            streamids.add(command.get(index));
                            index++;
                        }
                        int mid = streamids.size()/2;
                        entryids = streamids.subList(mid, streamids.size());
                        streamids = streamids.subList(0, mid);
                        if(streamids.size() != entryids.size()){
                            out.write("-ERR syntax error\r\n".getBytes());
                            out.flush();
                            continue;
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
                                        c++;
                                        if (c >= count || timeout != Long.MAX_VALUE) break;
                                    }

                                }
                                // write RESP array for this stream
                                if(result.size()!=0) results.add(result);
                            }
                        }while(timeout != Long.MAX_VALUE && results.size()==0 && (System.currentTimeMillis() - startTime) < timeout);

                        // if no results found after timeout
                        if(results.size()==0){
                            out.write("$-1\r\n".getBytes());
                            out.flush();
                            continue;
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
                    else {
                        out.write("-ERR unknown command\r\n".getBytes());
                        out.flush();
                    }
                }
            } catch (IOException e) {
                System.out.println(e);
            }
        }).start();

    }

    public static List<String> parseCommand(BufferedReader reader) throws IOException {
        String line;
        line = reader.readLine();
        List<String> command = new java.util.ArrayList<>();
        if (line != null && line.startsWith("*")) {
            int n = Integer.parseInt(line.substring(1));
            for (int i = 0; i < n; i++) {
                line = reader.readLine();
                line = reader.readLine();
                command.add(line);
            }
        }
        return command;
    }


    public static String getEntryId(List<String> command, OutputStream out, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue>>>  streamMap) throws IOException {
        // The entryid should be greater than the ID of the last entry in the stream.
        // The millisecondsTime part of the ID should be greater than or equal to the millisecondsTime of the last entry.
        // If the millisecondsTime part of the ID is equal to the millisecondsTime of the last entry, the sequenceNumber part of the ID should be greater than the sequenceNumber of the last entry.

        //When * is used with the XADD command, Redis auto-generates a unique auto-incrementing ID for the message being appended to the stream.
        //
        //Redis defaults to using the current unix time in milliseconds for the time part and 0 for the sequence number. If the time already exists in the stream, the sequence number for that record incremented by one will be used.

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
        if(lastEntryId!=null) lastEntryIdParts = lastEntryId.split("-");

        if(entryIdParts.length==1 && entryIdParts[0].equals("*")){
            // generate entry id
            long currentTimeMillis = System.currentTimeMillis();
            if(lastEntryIdParts.length>0 && Long.parseLong(lastEntryIdParts[0])==currentTimeMillis){
                entryid = lastEntryIdParts[0] + "-" + (Integer.parseInt(lastEntryIdParts[1]) + 1);
            } else {
                entryid = currentTimeMillis + "-0";
            }
            return entryid;
        }

        if (entryIdParts[1].equals("*")) {
            if (lastEntryIdParts.length>0 && entryIdParts[0].equals(lastEntryIdParts[0])) {
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
        if (lastEntryId!=null && (Integer.parseInt(entryIdParts[0]) < Integer.parseInt(lastEntryIdParts[0]) ||
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

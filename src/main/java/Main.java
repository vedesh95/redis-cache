import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Time;
import java.util.ArrayList;
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
      ConcurrentHashMap<String, Queue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
        try {
          serverSocket = new ServerSocket(port);
          // Since the tester restarts your program quite often, setting SO_REUSEADDR
          // ensures that we don't run into 'Address already in use' errors
          serverSocket.setReuseAddress(true);
          while (true){
            Socket clientSocket = serverSocket.accept();
            spinThread(clientSocket, map, lists, threadsWaitingForBLPOP);
          }
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        }
  }

    public static void spinThread(Socket clientSocket, ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, Queue<Thread>> threadsWaitingForBLPOP){
        new Thread(() -> {
            try (clientSocket;
                 BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 OutputStream out = clientSocket.getOutputStream()) {


                while(true){
                    List<String> command = parseCommand(reader);
                    if(command.isEmpty()) continue;
                    System.out.println("command is " + command);

                    if (command.get(0).equalsIgnoreCase("PING")) {
                        out.write("+PONG\r\n".getBytes());
                        out.flush();
                    } else if (command.get(0).startsWith("ECHO")) {
                        String line = command.get(1);
                        System.out.println("echo----" + line);
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

                        System.out.println("blpop command received " + command);
                        String key = command.get(1);
                        int timeout = Integer.parseInt(command.get(2)) * 1000; // convert to milliseconds
                        boolean waitForever = timeout == 0;
                        Thread currentThread = Thread.currentThread();
                        if (threadsWaitingForBLPOP.containsKey(key) == false) {
                            threadsWaitingForBLPOP.put(key, new ConcurrentLinkedQueue<>());
                        }
                        threadsWaitingForBLPOP.get(key).offer(currentThread);
                        System.out.println("----blpop----" + key + " " + timeout + " " + currentThread + threadsWaitingForBLPOP.get(key).size());
                        long startTime = System.currentTimeMillis();
                        while (waitForever || (System.currentTimeMillis() - startTime) < timeout && threadsWaitingForBLPOP.get(key).peek() == currentThread) {

                            if (lists.containsKey(key) && lists.get(key).isEmpty() == false) {
                                String value = lists.get(key).remove(0);
                                out.write(("*2\r\n$" + key.length() + "\r\n" + key + "\r\n" + "$" + value.length() + "\r\n" + value + "\r\n").getBytes());
                                out.flush();
                                threadsWaitingForBLPOP.get(key).remove();
                                break;
                            }
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
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

}

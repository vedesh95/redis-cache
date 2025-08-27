import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Time;
import java.util.HashMap;
import java.util.List;

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
public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

//      Uncomment this block to pass the first stage
        ServerSocket serverSocket = null;
//        Socket clientSocket = null;
        int port = 6379;
        try {
          serverSocket = new ServerSocket(port);
          // Since the tester restarts your program quite often, setting SO_REUSEADDR
          // ensures that we don't run into 'Address already in use' errors
          serverSocket.setReuseAddress(true);
          while (true){
            Socket clientSocket = serverSocket.accept();
            spinThread(clientSocket);
          }
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        }
  }

    public static void spinThread(Socket clientSocket) {
        new Thread(() -> {
            try (clientSocket;
                 BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 OutputStream out = clientSocket.getOutputStream()) {
                HashMap<String, Pair> map = new HashMap<>();
                HashMap<String, List<String>> lists = new HashMap<>();

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
                                lists.put(key, new java.util.ArrayList<>());
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
                            if(start < 0) start = 0;
                            if(start > end){
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
                    } else {
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

}

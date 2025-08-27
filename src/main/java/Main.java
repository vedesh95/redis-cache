import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;

class Pair{
    public String value;
    public Time time;
    public int expireTime;
    public Pair(String value, int expireTime) {
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

                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                    if (line.equalsIgnoreCase("PING")) {
                        out.write("+PONG\r\n".getBytes());
                        out.flush();
                    } else if (line.startsWith("ECHO")) {
                        String line0 = reader.readLine();
                        line = reader.readLine();
                        System.out.println("echo----" + line);
                        out.write((line0 + "\r\n" + line + "\r\n").getBytes());
                        out.flush();
                    } else if(line.contains("SET")){
                        String line0 = reader.readLine();
                        String line1 = reader.readLine();
                        String line2 = reader.readLine();
                        String line3 = reader.readLine();
                        String line4 = reader.readLine();
                        String line5 = reader.readLine();
                        String line6 = reader.readLine();
                        String line7 = reader.readLine();

                        System.out.println("set---" + line0 + " " + line1 + " " + line2 + " " + line3 + " " + line4 + " " + line5 + " " + line6 + " " + line7);
                        // px
//                        String line4 = reader.readLine();
//                        String line5 = reader.readLine();
//                        String line6 = reader.readLine();
//                        Integer line7 = Integer.valueOf((reader.readLine()));
//                        System.out.println("set----" + line1 + " " + line2 + " " + line3 + " " + line4 + " " + line5 + " " + line6 + " " + line7);
                        String key = line1;
                        String value = line3;
                        map.put(key, new Pair(value, Integer.valueOf(line3)));
                        out.write("+OK\r\n".getBytes());
                        out.flush();
                    } else if(line.contains("GET")){

                        String line0 = reader.readLine();
                        String line1 = reader.readLine();
                        System.out.println("get----" + line0 + " " + line1);
                        String key = line1;
                        if(map.containsKey(key) && map.get(key).expireTime + map.get(key).time.getTime() > System.currentTimeMillis()){
                            String value = map.get(key).value;
                            out.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
                            out.flush();
                        } else {
                            out.write("$-1\r\n".getBytes());
                            out.flush();
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

//    public List<List<String>> parse(BufferedReader reader) throws IOException {
//        String line;
//        line = reader.readLine();
//        ArrayList<ArrayList<String>> res = new ArrayList<>();
//        while (line != null) {
//            if (line.startsWith("*")) {
//                int n = Integer.parseInt(line.substring(1));
//                ArrayList<String> command = new ArrayList<>();
//                for (int i = 0; i < n; i++) {
//                    line = reader.readLine();
//                    int m = Integer.parseInt(line.substring(1));
//                    for(int j = 0; j < m; j++) {
//                        line = reader.readLine();
//                        if(line.contains("PING")){
//                            command.add("PING");
//                        }else if(line.contains("ECHO")) {
//                            command.add("ECHO");
//                        }else if(line.contains("SET"))
//                        command.add(line);
//                    }
//
//                }
//                res.add(command);
//            }
//            line = reader.readLine();
//        }
//
//    }

}

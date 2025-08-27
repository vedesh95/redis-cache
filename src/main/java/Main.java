import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Queue;

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
                HashMap<String, String> map = new HashMap<>();

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
                        System.out.println("set----" + line1 + " " + line2 + " " + line3);
                        String key = line1;
                        String value = line3;
                        map.put(key, value);
                        out.write("+OK\r\n".getBytes());
                        out.flush();
                    } else if(line.contains("GET")){

                        String line0 = reader.readLine();
                        String line1 = reader.readLine();
                        System.out.println("get----" + line0 + " " + line1);
                        String key = line1;
                        if(map.containsKey(key)){
                            String value = map.get(key);
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
}

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
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

  public static void spinThread(Socket clientSocket) throws IOException {
      new Thread(() -> {
          try(clientSocket) {
              while(true){
                  byte[] inp = new byte[1024];
                  int bytesRead = clientSocket.getInputStream().read(inp);
                  if (bytesRead == -1) break; // Client disconnected

                  String s = new String(inp,0, bytesRead);
                  System.out.println(s);
                  OutputStream out = clientSocket.getOutputStream();
                  out.write("+PONG\r\n".getBytes());
                  out.flush();

              }


          } catch (IOException e) {
              throw new RuntimeException(e);
          }
      }).start();
  }
}

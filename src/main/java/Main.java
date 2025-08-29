import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args){
        System.out.println("Logs from your program will appear here!");
        int port = 6379;
        if(args[0].equalsIgnoreCase("port")) port = Integer.parseInt(args[1]);
        System.out.println("Starting server on port: " + port);
        Cache cache = new Cache();
        ServerSocket serverSocket = null;

        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            while (true){
                Socket clientSocket = serverSocket.accept();
                cache.addClient(clientSocket);
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}


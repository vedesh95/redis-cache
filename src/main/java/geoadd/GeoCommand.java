package pubsub;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

public interface GeoCommand {
    public void execute(List<String> command, OutputStream out, Socket socket) throws IOException;
}
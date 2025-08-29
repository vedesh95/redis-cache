package command;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
public class Ping implements Command{

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        out.write("+PONG\r\n".getBytes());
        out.flush();
    }
}

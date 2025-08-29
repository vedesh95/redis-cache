package command;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class Echo implements Command{

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        String line = command.get(1);
        out.write(("$" + line.length() + "\r\n" + line + "\r\n").getBytes());
        out.flush();
    }
}

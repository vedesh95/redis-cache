package command;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public interface Command {
    public void execute(List<String> command, OutputStream out) throws IOException;
}

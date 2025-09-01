package command;

import struct.SortedSet;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class Zcard implements Command {
    private SortedSet sortedSet;

    public Zcard(SortedSet sortedSet) {
        this.sortedSet = sortedSet;
    }

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        out.write((":" + sortedSet.getZCard(command.get(1)) + "\r\n").getBytes());
    }
}
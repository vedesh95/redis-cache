package command;

import struct.SortedSet;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class Zrem implements Command{
    private SortedSet sortedSet;
    public Zrem(SortedSet sortedSet) {
        this.sortedSet = sortedSet;
    }

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        int ans =  this.sortedSet.getZRem(command.get(1), command.get(2));
        out.write((":" + ans + "\r\n").getBytes());
        out.flush();
    }
}

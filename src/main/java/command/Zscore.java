package command;

import struct.SortedSet;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class Zscore implements Command {
    private SortedSet sortedSet;

    public Zscore(SortedSet sortedSet) {
        this.sortedSet = sortedSet;
    }

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        String key = command.get(1);
        String member = command.get(2);
        double ans = this.sortedSet.getZScore(key, member);
        if (ans != -1) out.write((":" + ans + "\r\n").getBytes());
        else out.write(("$-1\r\n").getBytes());
        out.flush();
    }
}
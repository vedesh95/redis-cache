package command;

import struct.SortedSet;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class Zrange implements Command{
    private SortedSet sortedSet;
    public Zrange(SortedSet sortedSet){
        this.sortedSet = sortedSet;
    }
    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        // ZRANGE key start stop
        String key = command.get(1);
        int start = Integer.parseInt(command.get(2));
        int stop = Integer.parseInt(command.get(3));

        List<String> members = this.sortedSet.getRange(key, start, stop);
        out.write(("*" + members.size() + "\r\n").getBytes());
        for(String member : members){
            out.write(("$" + member.length() + "\r\n" + member + "\r\n").getBytes());
        }
        out.flush();



    }
}

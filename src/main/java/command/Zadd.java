package command;

import struct.SortedSet;
import struct.SortedSetElement;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class Zadd implements Command{

    private SortedSet sortedSet;
    public Zadd(SortedSet sortedSet){
        this.sortedSet = sortedSet;
    }
    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        String key = command.get(1);
        double score = Double.parseDouble(command.get(2));
        String member = command.get(3);

        int isNewMember = sortedSet.put(key, score, member);

        out.write((":" +isNewMember+"\r\n").getBytes());
        out.flush();
    }


}

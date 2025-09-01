package command;

import struct.SortedSetElement;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class Zadd implements Command{

    private Map<String, Set<SortedSetElement>> sortedSet;
    public Zadd(Map<String, Set<SortedSetElement>> sortedSet){
        this.sortedSet = sortedSet;
    }
    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        String key = command.get(1);
        double score = Double.parseDouble(command.get(2));
        String member = command.get(3);
        if(!sortedSet.containsKey(key)) sortedSet.put(key, new ConcurrentSkipListSet<SortedSetElement>());
        // check if member already exists otherwise add new element or update score
        Set<SortedSetElement> set = sortedSet.get(key);
        boolean updated = false;
        for(SortedSetElement element : set){
            if(element.member.equals(member)){
                set.remove(element);
                set.add(new SortedSetElement(score, member));
                updated = true;
                break;
            }
        }
        if(!updated){
            set.add(new SortedSetElement(score, member));
            out.write((":1\r\n").getBytes());
            out.flush();
            return;
        }
        out.write((":0\r\n").getBytes());
        out.flush();
    }


}

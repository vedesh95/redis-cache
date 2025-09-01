package struct;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class SortedSet {
    public Map<String, Map<String, Double>> sortedMembers;
    public Map<String, Map<Double, Set<String>>> scoreMembers;
    public SortedSet() {
        this.sortedMembers = new ConcurrentSkipListMap<>();
        this.scoreMembers = new ConcurrentSkipListMap<>();
    }

    public int put(String key, Double score, String member) {
        sortedMembers.putIfAbsent(key, new ConcurrentSkipListMap<>());
        scoreMembers.putIfAbsent(key, new ConcurrentSkipListMap<>());
        Map<String, Double> members = sortedMembers.get(key);
        Map<Double, Set<String>> scores = scoreMembers.get(key);
        int isNewMember = members.containsKey(member)==true ? 0 : 1;

        members.put(member, score);
        if(!scores.containsKey(score)) scores.put(score, new ConcurrentSkipListSet<>());
        scores.get(score).add(member);
        return isNewMember;
    }
}

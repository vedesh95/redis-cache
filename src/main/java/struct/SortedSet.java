package struct;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class SortedSet {
    public Map<String, Map<String, Double>> sortedMembers; // fast lookup by key member
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

    public int getRank(String key, String member) {
        if(!sortedMembers.containsKey(key) || !sortedMembers.get(key).containsKey(member)) return -1;
        double score = sortedMembers.get(key).get(member);
        Map<Double, Set<String>> scores = scoreMembers.get(key);
        int rank = 0;
        for(Map.Entry<Double, Set<String>> entry : scores.entrySet()) {
            if(entry.getKey() < score) {
                rank += entry.getValue().size();
            } else if(entry.getKey().equals(score)) {
                for(String m : entry.getValue()) {
                    if(m.equals(member)) {
                        return rank;
                    }
                    rank++;
                }
            } else {
                break;
            }
        }
        return rank;
    }

    public List<String> getRange(String key, int start, int end) {
        if(!sortedMembers.containsKey(key)) return new ArrayList<>();

        int size = sortedMembers.get(key).size();

        if(end >= size) end = size - 1;
        if(start < 0) start = size + start;
        if(end< 0) end = size + end;
        if(start < 0) start = 0;
        if(start >= size || start > end) return new ArrayList<>();

        Map<Double, Set<String>> scores = scoreMembers.get(key);
        List<String> result = new java.util.ArrayList<>();
        int index = 0;
        outer: for(Map.Entry<Double, Set<String>> entry : scores.entrySet()) {
            for (String member : entry.getValue()) {
                if (index >= start && index <= end) {
                    result.add(member);
                }
                index++;
                if (index > end) break outer;
            }
        }
        return result;
    }
}

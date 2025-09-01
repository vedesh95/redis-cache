package struct;

import java.util.Comparator;
import java.util.Objects;

public class SortedSetElement implements Comparable<SortedSetElement> {
    public double score;
    public String member;

    public SortedSetElement(double score, String member) {
        this.score = score;
        this.member = member;
    }

    @Override
    public int compareTo(SortedSetElement other) {
        int scoreComparison = Double.compare(this.score, other.score);
        if (scoreComparison != 0) {
            return scoreComparison;
        }
        return this.member.compareTo(other.member);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SortedSetElement that)) return false;
        return Double.compare(score, that.score) == 0 && Objects.equals(member, that.member);
    }

    @Override
    public int hashCode() {
        return Objects.hash(member, score);
    }
}

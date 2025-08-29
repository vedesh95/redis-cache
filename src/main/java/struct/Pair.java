package struct;

import java.sql.Time;

public class Pair{
    public String value;
    public Time time;
    public Integer expireTime;
    public Pair(String value, Integer expireTime) {
        this.value = value;
        time = new Time(System.currentTimeMillis());
        this.expireTime = expireTime;
    }
}

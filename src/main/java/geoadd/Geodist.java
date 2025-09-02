package geoadd;

import pubsub.GeoCommand;
import struct.SortedSet;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

public class Geodist implements GeoCommand {
    private SortedSet sortedSet;
    public Geodist(SortedSet sortedSet) {
        this.sortedSet = sortedSet;
    }
    @Override
    public void execute(List<String> command, OutputStream out, Socket socket) throws IOException {
        String key = command.get(1);
        String member1 = command.get(2);
        String member2 = command.get(3);
        double score1 = this.sortedSet.getZScore(key, member1);
        double score2 = this.sortedSet.getZScore(key, member2);
        if(score1 == -1 || score2 == -1){
            out.write("$-1\r\n".getBytes());
            out.flush();
            return;
        }
        List<Double> coords1 = RedisGeoCodec.decode((long) score1);
        List<Double> coords2 = RedisGeoCodec.decode((long) score2);
        double lon1 = coords1.get(0);
        double lat1 = coords1.get(1);
        double lon2 = coords2.get(0);
        double lat2 = coords2.get(1);
        double distance = RedisGeoDist.redisGeohashDistance(lat1, lon1, lat2, lon2);
        String distanceStr = String.valueOf(distance);
        out.write(("$" + distanceStr.length() + "\r\n").getBytes());
        out.write((distanceStr + "\r\n").getBytes());
        out.flush();

    }
}

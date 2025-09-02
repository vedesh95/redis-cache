package geoadd;

import pubsub.GeoCommand;
import struct.SortedSet;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

public class Geopos implements GeoCommand {
    private SortedSet sortedSet;
    public Geopos(SortedSet sortedSet) {
        this.sortedSet = sortedSet;
    }
    @Override
    public void execute(List<String> command, OutputStream out, Socket socket) throws IOException {
        String key = command.get(1);
        String member = command.get(2);
        double score = this.sortedSet.getZScore(key, member);
        if(score == -1){
            out.write(("*2\r\n*-1\r\n*-1\r\n").getBytes());
            out.flush();
            return;
        }
        List<Double> coords = RedisGeoCodec.decode((long) score);
        out.write(("*2\r\n".getBytes()));
        out.write(("$" + String.valueOf(coords.get(1)).length() + "\r\n").getBytes());
        out.write((String.valueOf(coords.get(1)) + "\r\n").getBytes());
        out.write(("$" + String.valueOf(coords.get(0)).length() + "\r\n").getBytes());
        out.write((String.valueOf(coords.get(0)) + "\r\n").getBytes());
        out.flush();
    }
}

package geoadd;

import pubsub.GeoCommand;
import struct.SortedSet;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Geopos implements GeoCommand {
    private SortedSet sortedSet;
    public Geopos(SortedSet sortedSet) {
        this.sortedSet = sortedSet;
    }
    @Override
    public void execute(List<String> command, OutputStream out, Socket socket) throws IOException {
        List<Double> res = new ArrayList<>();
        for(int i=1;i<=command.size()-2;i=i+2){
            String key = command.get(i);
            String member = command.get(i+1);
            double score = this.sortedSet.getZScore(key, member);
            if(score == -1){
                res.add(score);
                continue;
            }
            List<Double> coords = RedisGeoCodec.decode((long) score);
            res.addAll(coords);
        }
        // output res as array where each element is resp bulk string
        out.write(("*" + res.size() + "\r\n").getBytes());
        for(Double coord : res){
            if(coord == -1){
                out.write("$-1\r\n".getBytes());
            } else {
                String coordStr = String.valueOf(coord);
                out.write(("$" + coordStr.length() + "\r\n").getBytes());
                out.write((coordStr + "\r\n").getBytes());
            }
        }
        out.flush();
    }
}

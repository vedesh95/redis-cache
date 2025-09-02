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
        List<Double> coords = new ArrayList<>();
        for(int i=0;i<command.size();i=i+2){
            String key = command.get(1);
            String member = command.get(2);
            double score = this.sortedSet.getZScore(key, member);
            if(score == -1){
                out.write(("*2\r\n*-1\r\n*-1\r\n").getBytes());
                out.flush();
                return;
            }
            coords.addAll(RedisGeoCodec.decode((long) score));
        }
        // output
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(coords.size()).append("\r\n");
        for(double coord : coords){
            sb.append("$").append(String.valueOf(coord).length()).append("\r\n").append(coord).append("\r\n");
        }
        out.write(sb.toString().getBytes());
        out.flush();
    }
}

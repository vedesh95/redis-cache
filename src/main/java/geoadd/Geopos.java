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
        String key = command.get(0);
        out.write(("*" + (command.size() - 2) + "\r\n").getBytes());
        for(int i=1;i<=command.size();i++){
            String member = command.get(i);
            double score = this.sortedSet.getZScore(key, member);
            if(score == -1){
                out.write("*-1\r\n".getBytes());
                continue;
            }
            List<Double> coords = RedisGeoCodec.decode((long) score);
            // write coords as array of two bulk strings
            out.write(("*2\r\n").getBytes());
            for(Double coord : coords){
                String coordStr = String.valueOf(coord);
                out.write(("$" + coordStr.length() + "\r\n").getBytes());
                out.write((coordStr + "\r\n").getBytes());
            }
        }
        out.flush();
    }
}

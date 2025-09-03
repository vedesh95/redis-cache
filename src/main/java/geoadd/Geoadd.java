package geoadd;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import struct.SortedSet;

public class Geoadd implements GeoCommand {
    private SortedSet sortedSet;
    public Geoadd(SortedSet sortedSet) {
        this.sortedSet = sortedSet;
    }
    @Override
    public void execute(List<String> command, OutputStream out, Socket socket) throws IOException {
        String key = command.get(1);
        double longitude = Double.parseDouble(command.get(2));
        double latitude = Double.parseDouble(command.get(3));
        // check for valid latitude
        boolean isValidLongitude = (longitude >=RedisGeoCodec.MIN_LON  && longitude <= RedisGeoCodec.MAX_LON);
        boolean isValidLatitude = (latitude >=RedisGeoCodec.MIN_LAT  && latitude <= RedisGeoCodec.MAX_LAT);
        // if not valid, return error
        if (!isValidLongitude || !isValidLatitude) {
            out.write(("-ERR invalid latitude or longitude\r\n").getBytes());
            out.flush();
            return;
        }

        String member = command.get(4);
        double score = RedisGeoCodec.encode(longitude, latitude);
        int ans = this.sortedSet.put(key, score, member);
        // output
        out.write((":" + ans + "\r\n").getBytes());
        out.flush();
    }
}

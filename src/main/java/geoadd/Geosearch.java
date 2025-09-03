package geoadd;

import struct.SortedSet;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Objects;

public class Geosearch implements GeoCommand{
    private SortedSet sortedSet;
    public Geosearch(SortedSet sortedSet) {
        this.sortedSet = sortedSet;
    }

    @Override
    public void execute(List<String> command, OutputStream out, Socket socket) throws IOException {
        String name = command.get(1);
        double longitude = Double.parseDouble(command.get(3));
        double latitude = Double.parseDouble(command.get(4));
        double radius = Double.parseDouble(command.get(6));
        String unit = command.get(7);
        if(unit.equalsIgnoreCase("km")) radius = radius * 1000;
        else if(unit.equalsIgnoreCase("mi")) radius = radius * 1609.34;
        else if(unit.equalsIgnoreCase("ft")) radius = radius * 0.3048;
        else {
            out.write(("-ERR invalid unit\r\n").getBytes());
            out.flush();
            return;
        }
        List<String> members = RedisGeoCodec.geoSearch(longitude, latitude, radius, this.sortedSet.scoreMembers.get(name));
        out.write(("*" + members.size() + "\r\n").getBytes());
        for(String member : members){
            String memberStr = member;
            out.write(("$" + memberStr.length() + "\r\n").getBytes());
            out.write((memberStr + "\r\n").getBytes());
        }
    }
}

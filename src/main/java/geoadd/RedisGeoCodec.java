package geoadd;

import java.util.*;

public class RedisGeoCodec {

    public static final double MIN_LAT = -85.05112878;
    public static final double MAX_LAT = 85.05112878;
    public static final double MIN_LON = -180.0;
    public static final double MAX_LON = 180.0;
    private static final double LAT_RANGE = MAX_LAT - MIN_LAT;
    private static final double LON_RANGE = MAX_LON - MIN_LON;
    private static final long GRID_SIZE = 1L << 26; // 2^26
    private static final double EARTH_RADIUS_METERS = 6372797.560856;
    private static final double DEG_TO_RAD = Math.PI / 180.0;


    public static long encode(double lon, double lat) {
        long x = normalize(lat, MIN_LAT, LAT_RANGE);
        long y = normalize(lon, MIN_LON, LON_RANGE);
        return interleaveBits(x, y);
    }

    // step 1 2
    private static long normalize(double value, double min, double range) {
        double norm = GRID_SIZE * ((value - min) / range);
        return (long) norm; // truncation
    }

    // step 3
    private static long interleaveBits(long x, long y) {
        long a = (spreadBits(x));
        long b = (spreadBits(y) << 1);
        return a | b;
    }

    private static long spreadBits(long v) {
        v &= 0xFFFFFFFFL;
        v = (v | (v << 16)) & 0x0000FFFF0000FFFFL;
        v = (v | (v << 8))  & 0x00FF00FF00FF00FFL;
        v = (v | (v << 4))  & 0x0F0F0F0F0F0F0F0FL;
        v = (v | (v << 2))  & 0x3333333333333333L;
        v = (v | (v << 1))  & 0x5555555555555555L;
        return v;
    }

    public static List<Double> decode(long score) {
        long y = score >> 1, x = score;
        double gridLat = compactBits64To32(x);
        double gridLon = compactBits64To32(y);

        double latMin = cellMin(gridLat, MIN_LAT, LAT_RANGE);
        double latMax = cellMax(gridLat, MIN_LAT, LAT_RANGE);
        double lonMin = cellMin(gridLon, MIN_LON, LON_RANGE);
        double lonMax = cellMax(gridLon, MIN_LON, LON_RANGE);

        ArrayList<Double> coords = new ArrayList<>();
        coords.add((lonMin + lonMax)/2);
        coords.add((latMin+ latMax )/2);
        return coords;
    }

    private static double compactBits64To32(long v) {
        v &= 0x5555555555555555L;
        v = (v | (v >> 1))  & 0x3333333333333333L;
        v = (v | (v >> 2))  & 0x0F0F0F0F0F0F0F0FL;
        v = (v | (v >> 4))  & 0x00FF00FF00FF00FFL;
        v = (v | (v >> 8))  & 0x0000FFFF0000FFFFL;
        v = (v | (v >> 16)) & 0x00000000FFFFFFFFL;
        return v;
    }

    private static double cellMin(double grid, double min, double range) {
        return min + range * (grid / (double) GRID_SIZE);
    }

    private static double cellMax(double grid, double min, double range) {
        return min + range * ((grid + 1) / (double) GRID_SIZE);
    }

    public static double redisGeohashDistance(double lon1d, double lat1d,
                                              double lon2d, double lat2d) {
        double lon1r = lon1d * DEG_TO_RAD;
        double lon2r = lon2d * DEG_TO_RAD;
        double v = Math.sin((lon2r - lon1r) / 2.0);
        if (v == 0.0) {
            // optimization in Redis: if lon difference is (effectively) zero use lat distance
            return EARTH_RADIUS_METERS * Math.abs((lat2d - lat1d) * DEG_TO_RAD);
        }
        double lat1r = lat1d * DEG_TO_RAD;
        double lat2r = lat2d * DEG_TO_RAD;
        double u = Math.sin((lat2r - lat1r) / 2.0);
        double a = u * u + Math.cos(lat1r) * Math.cos(lat2r) * v * v;
        return 2.0 * EARTH_RADIUS_METERS * Math.asin(Math.sqrt(a));
    }

    public static double distanceBetweenScores(long score1, long score2) {
        List<Double> coord1 = decode(score1);
        List<Double> coord2 = decode(score2);
        double lon1 = coord1.get(0);
        double lat1 = coord1.get(1);
        double lon2 = coord2.get(0);
        double lat2 = coord2.get(1);
        return redisGeohashDistance(lon1, lat1, lon2, lat2);
    }

    public static List<String> geoSearch(double centerLon, double centerLat, double radiusMeters,
                                         Map<Double, Set<String>> scoreMembers) {
        List<String> results = new ArrayList<>();
        long centerScore = encode(centerLon, centerLat);

        for (Map.Entry<Double, Set<String>> entry : scoreMembers.entrySet()) {
            long score = entry.getKey().longValue();
            double dist = distanceBetweenScores(centerScore, score);
            if (dist <= radiusMeters) {
                results.addAll(entry.getValue());
            }
        }
        return results;
    }

//    public static void main(String[] args) {
//        double lon = -73.935242;
//        double lat = 40.730610;
//        long score = encode(lon, lat);
//        System.out.println("Encoded score: " + score);
//
//        List<Double> coords = decode(score);
//        System.out.println("Decoded coords: " + coords);
//
//        double lon2 = -74.935242;
//        double lat2 = 41.730610;
//        long score2 = encode(lon2, lat2);
//        double distance = distanceBetweenScores(score, score2);
//        System.out.println("Distance between points: " + distance + " meters");
//    }
}

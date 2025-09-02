package geoadd;

import struct.SortedSet;

import java.util.ArrayList;
import java.util.List;

public class RedisGeoCodec {

    private static final double MIN_LAT = -85.05112878;
    private static final double MAX_LAT = 85.05112878;
    private static final double MIN_LON = -180.0;
    private static final double MAX_LON = 180.0;
    private static final double LAT_RANGE = MAX_LAT - MIN_LAT;
    private static final double LON_RANGE = MAX_LON - MIN_LON;
    private static final long GRID_SIZE = 1L << 26; // 2^26

    public static long encode(double lat, double lon) {
        long x = normalize(lat, MIN_LAT, LAT_RANGE);
        long y = normalize(lon, MIN_LON, LON_RANGE);
        return interleaveBits(x, y);
    }

    private static long normalize(double value, double min, double range) {
        double norm = GRID_SIZE * ((value - min) / range);
        return (long) norm; // truncation
    }

    private static long interleaveBits(long x, long y) {
        long a = spreadBits(x);
        long b = spreadBits(y) << 1;
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

    public static double[] decodeToCellCenter(long score) {
        long y = score >> 1;      // longitude bits
        long x = score;           // latitude bits

        double gridLat = compactBits64To32(x);
        double gridLon = compactBits64To32(y);

        // exact match to repo: compute min/max of the cell, then take center
        double latMin = cellMin(gridLat, MIN_LAT, LAT_RANGE);
        double latMax = cellMax(gridLat, MIN_LAT, LAT_RANGE);
        double lonMin = cellMin(gridLon, MIN_LON, LON_RANGE);
        double lonMax = cellMax(gridLon, MIN_LON, LON_RANGE);

        double lat = (latMin + latMax) / 2.0;
        double lon = (lonMin + lonMax) / 2.0;
        return new double[] { lat, lon };
    }

    // If you prefer the direct “center” formula:
    public static double[] decodeToCellCenterFast(long score) {
        long y = score >> 1, x = score;
        double gridLat = compactBits64To32(x);
        double gridLon = compactBits64To32(y);
        double lat = MIN_LAT + LAT_RANGE * ((gridLat + 0.5) / (double) GRID_SIZE);
        double lon = MIN_LON + LON_RANGE * ((gridLon + 0.5) / (double) GRID_SIZE);
        return new double[] { lat, lon };
    }

    /* =========================
       Optional: get cell bounds
       ========================= */
    public static List<Double> decode(long score) {
        long y = score >> 1, x = score;
        double gridLat = compactBits64To32(x);
        double gridLon = compactBits64To32(y);

        double latMin = cellMin(gridLat, MIN_LAT, LAT_RANGE);
        double latMax = cellMax(gridLat, MIN_LAT, LAT_RANGE);
        double lonMin = cellMin(gridLon, MIN_LON, LON_RANGE);
        double lonMax = cellMax(gridLon, MIN_LON, LON_RANGE);

        //    latitude = (grid_latitude_min + grid_latitude_max) / 2
        //    longitude = (grid_longitude_min + grid_longitude_max) / 2
        ArrayList<Double> coords = new ArrayList<>();
        coords.add((latMin+ latMax )/2);
        coords.add((lonMin + lonMax)/2);
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

    public static void main(String[] args) {
        // Test example: New Delhi (28.6667, 77.2167)
        long code = encode(2.2944692, 48.8584625);
        System.out.println("Encoded score: " + code);

        List<Double> decoded = decode(code);
        System.out.printf("Decoded lat: %.6f, lon: %.6f%n", decoded.get(0), decoded.get(1));
    }
}

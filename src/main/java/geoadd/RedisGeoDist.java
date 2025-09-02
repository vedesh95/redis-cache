package geoadd;

public class RedisGeoDist {

    private static final double EARTH_RADIUS_METERS = 6372797.560856;
    private static final double DEG_TO_RAD = Math.PI / 180.0;

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

//    public static void main(String[] args) {
//        double lat1 = 48.164271, lon1 = 11.5030378;
//        double lat2 = 48.8584625, lon2 = 2.2944692;
//
//        long score1 = encodeScore(lat1, lon1);
//        long score2 = encodeScore(lat2, lon2);
//        double[] center1 = decodeCenterFromScore(score1);
//        double[] center2 = decodeCenterFromScore(score2);
//        double redisStyle = redisGeohashDistance(center1[1], center1[0], center2[1], center2[0]);
//
//        System.out.println(redisStyle);
//
////        double score1 = RedisGeoCodec.encode(lon1, lat1);
////        double score2 = RedisGeoCodec.encode(lon2, lat2);
//
//        score1 = RedisGeoCodec.encode(lon1, lat1);
//        score2 = RedisGeoCodec.encode(lon2, lat2);
//
//        lat1 = RedisGeoCodec.decode((long) score1).get(1);
//        lon1 = RedisGeoCodec.decode((long) score1).get(0);
//        lat2 = RedisGeoCodec.decode((long) score2).get(1);
//        lon2 = RedisGeoCodec.decode((long) score2).get(0);
//        redisStyle = redisGeohashDistance(lon1, lat1, lon2, lat2);
//        System.out.println(redisStyle);
//        System.out.println();
//    }
}
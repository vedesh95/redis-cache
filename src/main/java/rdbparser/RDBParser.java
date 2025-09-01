package rdbparser;

import java.io.*;
import java.util.*;

public class RDBParser {

    private static final String MAGIC = "REDIS";

    // --- Data Structures ---
    public static class RedisEntry {
        public String key;
        public String value;
        public Long expiry; // null if no expiry
        public int dbIndex;
        RedisEntry(String key, String value, Long expiry, int dbIndex) {
            this.key = key;
            this.value = value;
            this.expiry = expiry;
            this.dbIndex = dbIndex;
        }
    }

    private static class LengthOrEncoding {
        boolean isEncoding;
        long length;   // valid if !isEncoding
        int encoding;  // valid if isEncoding
    }


    public List<RedisEntry> entries = new ArrayList<>();

    // Metadata
    public Map<Integer, Integer> dbHashSizes = new HashMap<>();
    public Map<Integer, Integer> dbExpireSizes = new HashMap<>();
    public Map<String, String> auxMetadata = new LinkedHashMap<>();


    private LengthOrEncoding readLengthOrEncoding(DataInputStream dis) throws IOException {
        int first = dis.readUnsignedByte();
        int type = (first & 0xC0) >> 6; // top 2 bits

        LengthOrEncoding result = new LengthOrEncoding();

        if (type == 0) {
            result.isEncoding = false;
            result.length = first & 0x3F;
        } else if (type == 1) {
            int second = dis.readUnsignedByte();
            result.isEncoding = false;
            result.length = ((first & 0x3F) << 8) | second;
        } else if (type == 2) {
            result.isEncoding = false;
            result.length = dis.readInt() & 0xFFFFFFFFL;
        } else { // 0b11 => special encoding
            result.isEncoding = true;
            result.encoding = first & 0x3F;
        }
        return result;
    }


    private long readUnsignedIntLE(DataInputStream dis) throws IOException {
        long b1 = dis.readUnsignedByte();
        long b2 = dis.readUnsignedByte();
        long b3 = dis.readUnsignedByte();
        long b4 = dis.readUnsignedByte();
        return (b1) | (b2 << 8) | (b3 << 16) | (b4 << 24);
    }

    private long readLongLE(DataInputStream dis) throws IOException {
        long b1 = dis.readUnsignedByte();
        long b2 = dis.readUnsignedByte();
        long b3 = dis.readUnsignedByte();
        long b4 = dis.readUnsignedByte();
        long b5 = dis.readUnsignedByte();
        long b6 = dis.readUnsignedByte();
        long b7 = dis.readUnsignedByte();
        long b8 = dis.readUnsignedByte();
        return (b1) | (b2 << 8) | (b3 << 16) | (b4 << 24) |
                (b5 << 32) | (b6 << 40) | (b7 << 48) | (b8 << 56);
    }


    private String readString(DataInputStream dis) throws IOException {
        LengthOrEncoding lenOrEnc = readLengthOrEncoding(dis);

        if (!lenOrEnc.isEncoding) {
            // Normal string
            byte[] buf = new byte[(int) lenOrEnc.length];
            dis.readFully(buf);
            return new String(buf, "UTF-8");
        } else {
            switch (lenOrEnc.encoding) {
                case 0: // 8-bit int
                    return String.valueOf(dis.readByte());
                case 1: // 16-bit int (LE)
                    int i16 = dis.readUnsignedByte() | (dis.readUnsignedByte() << 8);
                    return String.valueOf(i16);
                case 2: // 32-bit int (LE)
                    int i32 = dis.readUnsignedByte()
                            | (dis.readUnsignedByte() << 8)
                            | (dis.readUnsignedByte() << 16)
                            | (dis.readUnsignedByte() << 24);
                    return String.valueOf(i32);
                case 3: // LZF compressed string
                    throw new IOException("LZF compression not supported in this implementation");
                    // not encountered in codecrafters tests
//                    long compressedLen = readLengthOrEncoding(dis).length;
//                    long uncompressedLen = readLengthOrEncoding(dis).length;
//                    byte[] compressed = new byte[(int) compressedLen];
//                    dis.readFully(compressed);
//                    byte[] uncompressed = lzfDecompress(compressed, (int) uncompressedLen);
//                    return new String(uncompressed, "UTF-8");
                default:
                    throw new IOException("Unknown string encoding type: " + lenOrEnc.encoding);
            }
        }
    }


    public void parse(String filePath) throws IOException {
        try (DataInputStream dis = new DataInputStream(new FileInputStream(filePath))) {

            // --- 1. Header ---
            byte[] magicBytes = new byte[5];
            dis.readFully(magicBytes);
            String magic = new String(magicBytes, "ASCII");
            if (!MAGIC.equals(magic)) {
                throw new IOException("Invalid RDB file. Magic mismatch: " + magic);
            }
            byte[] versionBytes = new byte[4];
            dis.readFully(versionBytes);
            String version = new String(versionBytes, "ASCII");
            System.out.println("Magic: " + magic + ", Version: " + version);

            // --- 2. Parse body ---
            int currentDb = 0;
            Long expiry = null;

            while (true) {
                int opcode;
                try {
                    opcode = dis.readUnsignedByte();
                } catch (EOFException e) {
                    break;
                }

                if (opcode == 0xFF) { // EOF
                    System.out.println("End of RDB file.");
                    break;
                }

                switch (opcode) {
                    case 0xFE: { // SELECTDB
                        long dbIndex = readLengthOrEncoding(dis).length;
                        currentDb = (int) dbIndex;
                        System.out.println("Switched to DB " + currentDb);
                        break;
                    }
                    case 0xFB: { // RESIZEDB
                        long hashTableSize = readLengthOrEncoding(dis).length;
                        long expiryTableSize = readLengthOrEncoding(dis).length;
                        dbHashSizes.put(currentDb, (int) hashTableSize);
                        dbExpireSizes.put(currentDb, (int) expiryTableSize);
                        System.out.println("DB " + currentDb + " resizedb: main=" +
                                hashTableSize + " expire=" + expiryTableSize);
                        break;
                    }
                    case 0xFD: { // Expire time in seconds (LE)
                        expiry = readUnsignedIntLE(dis);
                        int valueType = dis.readUnsignedByte();
                        if (valueType != 0x00) throw new IOException("Only string supported");
                        String key = readString(dis);
                        String value = readString(dis);
                        entries.add(new RedisEntry(key, value, expiry*1000, currentDb));
                        expiry = null;
                        break;
                    }
                    case 0xFC: { // Expire time in ms (LE)
                        expiry = readLongLE(dis);
                        int valueType = dis.readUnsignedByte();
                        if (valueType != 0x00) throw new IOException("Only string supported");
                        String key = readString(dis);
                        String value = readString(dis);
                        entries.add(new RedisEntry(key, value, expiry, currentDb));
                        expiry = null;
                        break;
                    }

                    case 0x00: { // String key-value
                        String key = readString(dis);
                        String value = readString(dis);
                        entries.add(new RedisEntry(key, value, null, currentDb));
                        break;
                    }
                    case 0xFA: { // AUX metadata
                        String auxKey = readString(dis);
                        String auxValue = readString(dis);
                        auxMetadata.put(auxKey, auxValue);
                        System.out.println("AUX metadata: " + auxKey + " = " + auxValue);
                        break;
                    }

                    default:
                        throw new IOException("Unsupported opcode/type: 0x" +
                                Integer.toHexString(opcode));
                }
            }
        }
//        printData();
    }

    public void printData() {
        System.out.println("\n--- Parsed Entries ---");
        for (RedisEntry entry : entries) {
            if (entry.expiry != null) {
                System.out.println("[DB " + entry.dbIndex + "] " + entry.key +
                        " -> " + entry.value + " (expires at " + entry.expiry + ")");
            } else {
                System.out.println("[DB " + entry.dbIndex + "] " + entry.key +
                        " -> " + entry.value);
            }
        }

        System.out.println("\n--- DB Metadata ---");
        for (int db : dbHashSizes.keySet()) {
            System.out.println("DB " + db + ": mainHash=" + dbHashSizes.get(db) +
                    " expireHash=" + dbExpireSizes.get(db));
        }
    }

//    public static void main(String[] args) throws IOException {
//        rdbparser.RDBParser parser = new rdbparser.RDBParser();
//        parser.parse("/Users/vedesh/vedeshg/codecrafters-redis-java/dump.rdb");
//        parser.printData();
//    }


}

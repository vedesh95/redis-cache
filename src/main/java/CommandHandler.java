import command.*;
import geoadd.*;
import pubsub.*;
import rdbparser.RDBParser;
import struct.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class CommandHandler {
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue>>> streamMap = new ConcurrentHashMap<>();
    private ServerInfo info;
    private AtomicInteger ackCounter;
    private RDBDetails rdbDetails;
    private RDBParser rdbparser;
    private Map<String, java.util.Set<Socket> > pubSubMap;
    private Map<Socket, java.util.Set<String>> subPubMap;
    private SortedSet sortedSet;

    Command ping;
    Command echo;
    Command set;
    Command get;
    Command rpush;
    Command lrange;
    Command lpush;
    Command llen;
    Command lpop;
    Command blpop;
    Command type;
    Command xadd;
    Command xrange;
    Command xread;
    Command incr;
    Command replicationInfo;
    Command config;
    Command keys;
    // pubsub commands
    PubSubCommand subscribe;
    PubSubCommand publish;
    PubSubCommand pubsubPing;
    PubSubCommand unsubscribe;
    // sortedset commands
    Command zadd;
    Command zrank;
    Command zrange;
    Command zcard;
    Command zscore;
    Command zrem;
    // geo commands
    GeoCommand geoadd;
    GeoCommand geopos;
    GeoCommand geodist;
    GeoCommand geosearch;

    public CommandHandler(ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue>>> streamMap, ServerInfo info, AtomicInteger ackCounter, RDBDetails rdbDetails, RDBParser rdbparser, Map<String, java.util.Set<Socket> > pubSubMap, Map<Socket, java.util.Set<String>> subPubMap, SortedSet sortedSet) {
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
        this.info = info;
        this.ackCounter = ackCounter;
        this.rdbDetails = rdbDetails;
        this.rdbparser = rdbparser;
        this.pubSubMap = pubSubMap;
        this.subPubMap = subPubMap;
        this.sortedSet = sortedSet;

        this.ping = new Ping();
        this.echo = new Echo();
        this.set = new Set(map, lists, threadsWaitingForBLPOP, streamMap);
        this.get = new Get(map, lists, threadsWaitingForBLPOP, streamMap, rdbparser);
        this.rpush = new Rpush(map, lists, threadsWaitingForBLPOP, streamMap);
        this.lrange = new Lrange(map, lists, threadsWaitingForBLPOP, streamMap);
        this.lpush = new Lpush(map, lists, threadsWaitingForBLPOP, streamMap);
        this.llen = new Llen(map, lists, threadsWaitingForBLPOP, streamMap);
        this.lpop = new Lpop(map, lists, threadsWaitingForBLPOP, streamMap);
        this.blpop = new Blpop(map, lists, threadsWaitingForBLPOP, streamMap);
        this.type = new Type(map, lists, threadsWaitingForBLPOP, streamMap);
        this.xadd = new Xadd(map, lists, threadsWaitingForBLPOP, streamMap);
        this.xrange = new Xrange(map, lists, threadsWaitingForBLPOP, streamMap);
        this.xread = new Xread(map, lists, threadsWaitingForBLPOP, streamMap);
        this.incr = new Incr(map, lists, threadsWaitingForBLPOP, streamMap);
        this.replicationInfo = new ReplicationInfo(info);
        this.config = new Config(map, lists, threadsWaitingForBLPOP, streamMap, rdbDetails);
        this.keys = new Keys(map, lists, threadsWaitingForBLPOP, streamMap, rdbDetails, rdbparser);

        this.subscribe = new Subscribe(map, lists, threadsWaitingForBLPOP, streamMap, rdbDetails, rdbparser, pubSubMap, subPubMap);
        this.pubsubPing = new PubSubPing(map, lists, threadsWaitingForBLPOP, streamMap, rdbDetails, rdbparser, pubSubMap, subPubMap);
        this.publish = new pubsub.Publish(map, lists, threadsWaitingForBLPOP, streamMap, rdbDetails, rdbparser, pubSubMap, subPubMap);
        this.unsubscribe = new UnSubscribe(map, lists, threadsWaitingForBLPOP, streamMap, rdbDetails, rdbparser, pubSubMap, subPubMap);

        this.zadd = new Zadd(sortedSet);
        this.zrank = new Zrank(sortedSet);
        this.zrange = new Zrange(sortedSet);
        this.zcard = new Zcard(sortedSet);
        this.zscore = new Zscore(sortedSet);
        this.zrem = new Zrem(sortedSet);

        this.geoadd = new Geoadd(sortedSet);
        this.geopos = new Geopos(sortedSet);
        this.geodist = new Geodist(sortedSet);
        this.geosearch = new Geosearch(sortedSet);
    }

    public void handleCommand(List<String> command, OutputStream out, Socket socket){
        try{
            switch (command.get(0).toUpperCase(Locale.ROOT)) {
                case "PING":
                    if(!this.subPubMap.containsKey(socket)) {ping.execute(command, out); break;}
                    pubsubPing.execute(command, out, socket);break;
                case "ECHO": echo.execute(command, out); break;
                case "SET": set.execute(command, out); break;
                case "GET": get.execute(command, out); break;
                case "RPUSH": rpush.execute(command, out); break;
                case "LPUSH": lpush.execute(command, out); break;
                case "LRANGE": lrange.execute(command, out); break;
                case "LLEN": llen.execute(command, out); break;
                case "LPOP": lpop.execute(command, out); break;
                case "BLPOP": blpop.execute(command, out); break;
                case "TYPE": type.execute(command, out); break;
                case "XADD": xadd.execute(command, out); break;
                case "XRANGE": xrange.execute(command, out); break;
                case "XREAD": xread.execute(command, out); break;
                case "INCR": incr.execute(command, out); break;
                case "INFO": replicationInfo.execute(command, out); break;
                case "REPLCONF":
                    // takes care of flow when replica is connecting to master
                    if(command.get(1).equalsIgnoreCase("GETACK")){
                        out.write(("+REPLCONF ACK 0\r\n").getBytes());
                        out.flush();
                        break;
                    }else if(command.get(1).equalsIgnoreCase("LISTENING-PORT") || command.get(1).equalsIgnoreCase("CAPA") || command.get(1).equalsIgnoreCase("IP")){
                        out.write("+OK\r\n".getBytes());
                        out.flush();
                        break;
                    }else if(command.get(1).equalsIgnoreCase("ACK")){
                        this.ackCounter.incrementAndGet();
                        break;
                    }
                    break;
                case "PSYNC":
                    // send +FULLRESYNC <REPL_ID> <master_repl_offset>\r\n
                    out.write(("+FULLRESYNC " + this.info.getMaster_replid() + " " + this.info.getMaster_repl_offset() + "\r\n").getBytes());
                    out.flush();

                    byte[] contents = java.util.Base64.getDecoder().decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==");
                    out.write(("$" + contents.length + "\r\n").getBytes());
                    out.write(contents);
                    out.flush();
                    break;
                case "CONFIG": config.execute(command, out); break;
                case "KEYS": keys.execute(command, out); break;
                case "SUBSCRIBE": subscribe.execute(command, out, socket); break;
                case "PUBLISH": publish.execute(command, out, socket); break;
                case "UNSUBSCRIBE": unsubscribe.execute(command, out, socket); break;
                case "ZADD": zadd.execute(command, out); break;
                case "ZRANK": zrank.execute(command, out); break;
                case "ZRANGE": zrange.execute(command, out); break;
                case "ZCARD": zcard.execute(command, out); break;
                case "ZSCORE": zscore.execute(command, out); break;
                case "ZREM": zrem.execute(command, out); break;
                case "GEOADD": geoadd.execute(command, out, socket); break;
                case "GEOPOS": geopos.execute(command, out, socket); break;
                case "GEODIST": geodist.execute(command, out, socket); break;
                case "GEOSEARCH": geosearch.execute(command, out, socket); break;
                case "QUIT":
                    out.write("+OK\r\n".getBytes());
                    out.flush();
                    socket.close();
                    break;
                default:
                    out.write("-ERR unknown command\r\n".getBytes());
                    out.flush();
                    break;
            }

        }catch (Exception e){
            System.out.println(e);
        }
    }

    public void propagateToSlaves(List<String> command, OutputStream out) throws IOException {
        // list of commands to be propogated to replicas
//        if(command.get(0).equalsIgnoreCase("SET")) System.out.println(command + " " + command.getClass().getSimpleName());
        List<String> commandsToPropogate = List.of("SET", "GET", "RPUSH", "LPUSH", "LPOP", "BLPOP", "XADD", "INCR");
        if(commandsToPropogate.contains(command.get(0).toUpperCase(Locale.ROOT))){
            // write command to out
            StringBuilder sb = new StringBuilder();
            sb.append("*").append(command.size()).append("\r\n");
            for(String arg : command){
                sb.append("$").append(arg.length()).append("\r\n");
                sb.append(arg).append("\r\n");
            }
            out.write(sb.toString().getBytes());
            out.flush();
        }

    }
}

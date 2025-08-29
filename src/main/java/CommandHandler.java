import command.*;
import struct.KeyValue;
import struct.Pair;

import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CommandHandler {
    private ConcurrentHashMap<String, Pair> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue>>> streamMap = new ConcurrentHashMap<>();
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

    public CommandHandler(ConcurrentHashMap<String, Pair> map, ConcurrentHashMap<String, List<String>> lists, ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>> threadsWaitingForBLPOP, ConcurrentHashMap<String, LinkedHashMap<String, List<KeyValue>>> streamMap) {
        this.map = map;
        this.lists = lists;
        this.threadsWaitingForBLPOP = threadsWaitingForBLPOP;
        this.streamMap = streamMap;
        this.ping = new Ping();
        this.echo = new Echo();
        this.set = new Set(map, lists, threadsWaitingForBLPOP, streamMap);
        this.get = new Get(map, lists, threadsWaitingForBLPOP, streamMap);
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
    }

    public void handleCommand(List<String> command, OutputStream out){
        try{
            switch (command.get(0).toUpperCase(Locale.ROOT)) {
                case "PING": ping.execute(command, out); break;
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
                default:
                    out.write("-ERR unknown command\r\n".getBytes());
                    out.flush();
                    break;
            }
        }catch (Exception e){
            // log exception
            System.out.println(e);
        }
    }
}

package command;

import struct.ServerInfo;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class ReplicationInfo implements Command{
    private ServerInfo serverInfo;
    public ReplicationInfo(ServerInfo serverInfo){
        this.serverInfo = serverInfo;
    }

    @Override
    public void execute(List<String> command, OutputStream out) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("# Replication\r\n");
        sb.append("role:").append(serverInfo.getRole()).append("\r\n");
//        sb.append("connected_slaves:").append(serverInfo.getConnected_slaves()).append("\r\n");
//        sb.append("master_replid:").append(serverInfo.getMaster_replid()).append("\r\n");
//        sb.append("master_repl_offset:").append(serverInfo.getMaster_repl_offset()).append("\r\n");
//        sb.append("second_repl_offset:").append(serverInfo.getSecond_repl_offset()).append("\r\n");
//        sb.append("repl_backlog_active:").append(serverInfo.getRepl_backlog_active()).append("\r\n");
//        sb.append("repl_backlog_size:").append(serverInfo.getRepl_backlog_size()).append("\r\n");
//        sb.append("repl_backlog_first_byte_offset:").append(serverInfo.getRepl_backlog_first_byte_offset()).append("\r\n");
//        sb.append("repl_backlog_histlen:").append(serverInfo.getRepl_backlog_histlen()).append("\r\n");

        String response = sb.toString();
        out.write(("$" + response.length() + "\r\n" + response).getBytes());
        out.flush();
    }
}

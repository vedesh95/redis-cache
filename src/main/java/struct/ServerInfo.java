package struct;

public class ServerInfo {
    private String role;
    private int connected_slaves;
    private String master_replid;
    private long master_repl_offset;
    private long second_repl_offset;
    private int repl_backlog_active;
    private int repl_backlog_size;
    private long repl_backlog_first_byte_offset;
    private Long repl_backlog_histlen;

    public ServerInfo(String role, int connected_slaves, String master_replid, long master_repl_offset, long second_repl_offset, int repl_backlog_active, int repl_backlog_size, long repl_backlog_first_byte_offset, Long repl_backlog_histlen) {
        this.role = role;
        this.connected_slaves = connected_slaves;
        this.master_replid = master_replid;
        this.master_repl_offset = master_repl_offset;
        this.second_repl_offset = second_repl_offset;
        this.repl_backlog_active = repl_backlog_active;
        this.repl_backlog_size = repl_backlog_size;
        this.repl_backlog_first_byte_offset = repl_backlog_first_byte_offset;
        this.repl_backlog_histlen = repl_backlog_histlen;
    }

    public ServerInfo(){
        this.role = "master";
        this.connected_slaves = 0;
        this.master_replid = "0000000000000000000000000000000000000000";
        this.master_repl_offset = 0;
        this.second_repl_offset = 0;
        this.repl_backlog_active = 0;
        this.repl_backlog_size = 0;
        this.repl_backlog_first_byte_offset = 0;
        this.repl_backlog_histlen = null;
    }

    public int getConnected_slaves() {
        return connected_slaves;
    }

    public void setConnected_slaves(int connected_slaves) {
        this.connected_slaves = connected_slaves;
    }

    public long getMaster_repl_offset() {
        return master_repl_offset;
    }

    public void setMaster_repl_offset(long master_repl_offset) {
        this.master_repl_offset = master_repl_offset;
    }

    public String getMaster_replid() {
        return master_replid;
    }

    public void setMaster_replid(String master_replid) {
        this.master_replid = master_replid;
    }

    public int getRepl_backlog_active() {
        return repl_backlog_active;
    }

    public void setRepl_backlog_active(int repl_backlog_active) {
        this.repl_backlog_active = repl_backlog_active;
    }

    public long getRepl_backlog_first_byte_offset() {
        return repl_backlog_first_byte_offset;
    }

    public void setRepl_backlog_first_byte_offset(long repl_backlog_first_byte_offset) {
        this.repl_backlog_first_byte_offset = repl_backlog_first_byte_offset;
    }

    public Long getRepl_backlog_histlen() {
        return repl_backlog_histlen;
    }

    public void setRepl_backlog_histlen(Long repl_backlog_histlen) {
        this.repl_backlog_histlen = repl_backlog_histlen;
    }

    public int getRepl_backlog_size() {
        return repl_backlog_size;
    }

    public void setRepl_backlog_size(int repl_backlog_size) {
        this.repl_backlog_size = repl_backlog_size;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public long getSecond_repl_offset() {
        return second_repl_offset;
    }

    public void setSecond_repl_offset(long second_repl_offset) {
        this.second_repl_offset = second_repl_offset;
    }
}

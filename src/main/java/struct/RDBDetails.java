package struct;

public class RDBDetails {
    private String dir;
    private String dbfilename;

    public RDBDetails(String dir, String dbfilename) {
        this.dir = dir;
        this.dbfilename = dbfilename;
    }

    public String getDir() {
        return dir;
    }
    public String getDbfilename() {
        return dbfilename;
    }

    public void setDbfilename(String dbfilename) {
        this.dbfilename = dbfilename;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }
}

package struct;

import java.io.BufferedReader;
import java.io.OutputStream;

public class SlaveDetails {
    private Integer x;
    private BufferedReader reader;
    private OutputStream outputStream;

    public SlaveDetails(Integer x, BufferedReader reader, OutputStream outputStream){
        this.x = x;
        this.reader = reader;
        this.outputStream = outputStream;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public void setOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public BufferedReader getReader() {
        return reader;
    }

    public void setReader(BufferedReader reader) {
        this.reader = reader;
    }

    public Integer getX() {
        return x;
    }

    public void setX(Integer x) {
        this.x = x;
    }
}

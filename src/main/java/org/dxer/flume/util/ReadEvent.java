package org.dxer.flume.util;

import java.io.File;
import java.io.Serializable;

/**
 * Created by linghf on 2017/2/15.
 */

public class ReadEvent implements Serializable {

    private File readFile;
    private String line;
    private long lineNum;
    private long filePointer;

    public ReadEvent(File file, String line, long lineNum, long filePointer) {
        this.readFile = readFile;
        this.lineNum = lineNum;
        this.filePointer = filePointer;
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public File getReadFile() {
        return readFile;
    }

    public void setReadFile(File readFile) {
        this.readFile = readFile;
    }

    public long getLineNum() {
        return lineNum;
    }

    public void setLineNum(long lineNum) {
        this.lineNum = lineNum;
    }

    public long getFilePointer() {
        return filePointer;
    }

    public void setFilePointer(long filePointer) {
        this.filePointer = filePointer;
    }
}

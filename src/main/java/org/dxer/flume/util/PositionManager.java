package org.dxer.flume.util;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;

/**
 * Created by linghf on 2017/2/15.
 */

public class PositionManager {

    private static final Logger logger = LoggerFactory.getLogger(PositionManager.class);

    private RandomAccessFile reader;

    private String posFile;

    private int lineNum = 0;

    public PositionManager(String posFile) {
        this.posFile = posFile;
    }

    public RandomAccessFile getReader() {
        return reader;
    }

    public void setReader(RandomAccessFile reader) {
        this.reader = reader;
    }

    public String getPosFile() {
        return posFile;
    }

    public void setPosFile(String posFile) {
        this.posFile = posFile;
    }


    public static String readLastLine(RandomAccessFile reader) throws Exception {
        if (reader != null) {

            try {
                long len = reader.length();
                if (len == 0L) {
                    return "";
                } else {
                    long pos = len - 1;
                    while (pos > 0) {
                        pos--;
                        reader.seek(pos);
                        if (reader.readByte() == '\n') {
                            break;
                        }
                    }
                    if (pos == 0) {
                        reader.seek(0);
                    }
                    byte[] bytes = new byte[(int) (len - pos)];
                    reader.read(bytes);
                    return new String(bytes);
                }
            } catch (Exception e) {
                throw e;
            }
        }
        return null;
    }


    public long getStartPosition() {
        if (reader == null) {
            try {
                reader = new RandomAccessFile(posFile, "rw");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            String lastLine = readLastLine(reader);
            if (!Strings.isNullOrEmpty(lastLine)) {
                String[] strs = lastLine.split("\\|");

                long last = Long.parseLong(strs[0]);
                long lineNum = Long.parseLong(strs[1]);
                long pointer = Long.parseLong(strs[2]);
                return pointer + 1;
            }
        } catch (Exception e) {

        }
        return 0;
    }

    public void recordPosition(ReadEvent readEvent) {
        if (reader == null) {
            try {
                reader = new RandomAccessFile(posFile, "rw");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

       // logger.info("recordPosition");

        try {
            if (lineNum == 9999) {
                reader.setLength(0);
                reader.seek(0);
            } else {
                reader.seek(reader.length());
            }

            File file = readEvent.getReadFile();
            String fileKey = readEvent.getFileKey();
            long lineNum = readEvent.getLineNum();
            long pointer = readEvent.getFilePointer();

            StringBuilder content = new StringBuilder();
            content.append(fileKey).append("|").append(lineNum).append("|").append(pointer).append("\n");

            reader.write(content.toString().getBytes());
            lineNum++;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (reader != null) {
            try {
                reader.close();
            } catch (Exception e) {
            }
        }
    }

    public static void main(String[] args) {
        File file = new File("D:\\11\\logs\\testlog.log");
        System.out.println(file.lastModified());
    }
}

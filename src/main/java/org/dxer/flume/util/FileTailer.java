package org.dxer.flume.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by linghf on 2017/2/7.
 */

public class FileTailer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(FileTailer.class);

    private static final int DEFAULT_DELAY_MILLIS = 1000;

    private static final String RAF_MODE = "r";

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    private String tailFileName;

    private Charset charset = Charset.defaultCharset();

    private long delayMillis;

    private Long startPosition = null;

    /**
     * The tailer will run as long as this value is true.
     */
    private volatile boolean running = true;

    private List<FileTailerHandler> handlers = new ArrayList<FileTailerHandler>();

    public FileTailer(String tailFileName) {
        this(tailFileName, DEFAULT_CHARSET, DEFAULT_DELAY_MILLIS);
    }

    public FileTailer(String tailFileName, long delayMillis) {
        this(tailFileName, DEFAULT_CHARSET, delayMillis);
    }

    public FileTailer(String tailFileName, Charset charset, long delayMillis) {
        this.tailFileName = tailFileName;
        this.charset = charset;
        this.delayMillis = delayMillis;
    }

    public Long getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(Long startPosition) {
        this.startPosition = startPosition;
    }

    public void addFileTailerHandler(FileTailerHandler handler) {
        this.handlers.add(handler);
    }

    public void removeFileTailerHandler(FileTailerHandler handler) {
        this.handlers.remove(handler);
    }

    public void run() {
        RandomAccessFile reader = null;

        long last = 0; // The last time the file was checked for changes
        long position = 0l; //  getPosition(file); // position within the file
        long lineNum = 0;

        boolean isFirst = true;

        String line = null;
        while (isRunning()) {
            while (reader == null) {
                try {
                    reader = new RandomAccessFile(tailFileName, RAF_MODE);
                } catch (final FileNotFoundException e) {

                }
                if (reader == null) {
                    sleep(delayMillis);
                }
            }
            try {
                File file = new File(tailFileName);
                final boolean newer = FileUtils.isFileNewer(file, last); // IO-279, must be done first

                if (newer) { // if is a new file
                    lineNum = 0l;
                    last = file.lastModified();
                    if (isFirst && startPosition != null && startPosition.longValue() > 0) {
                        position = startPosition;
                        isFirst = false;
                    } else {
                        position = 0l; // a new file, set postition to 0
                    }
                    logger.info("tail a new file: " + tailFileName + ", last: " + last);
                }


                reader.seek(position);

                while ((line = reader.readLine()) != null) { // read file
                    line = new String(line.getBytes(charset), "UTF-8"); //编码转换
                    lineNum = lineNum + 1;
                    long filePoiner = reader.getFilePointer();
                    ReadEvent readEvent = new ReadEvent(file, line, lineNum, filePoiner);
                    handle(readEvent); // handle line
                }
                sleep(delayMillis);
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("tail file error, " + e.getMessage());
            } finally {
                IOUtils.closeQuietly(reader);
                reader = null;
            }
        }
    }
    
    public void stop() {
        this.running = false;
    }

    protected boolean isRunning() {
        return running;
    }

    public long getDelay() {
        return delayMillis;
    }

    public String getFileName() {
        return this.tailFileName;
    }

    private void sleep(long delayMillis) {
        try {
            Thread.sleep(delayMillis);
        } catch (Exception e) {
        }
    }

    private void handle(ReadEvent readEvent) {
        if (readEvent != null && handlers != null && !handlers.isEmpty()) {
            for (FileTailerHandler handler : handlers) {
                handler.process(readEvent);
            }
        }
    }


    public static void main(String[] args) {
        FileTailer tailer = new FileTailer("/home/hadoop/flume/logs/testlog.log", 500l);
        new Thread(tailer).start();
    }
}

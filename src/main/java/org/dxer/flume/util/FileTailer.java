package org.dxer.flume.util;

import com.google.common.base.Strings;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    private TailReader tailReader;

    private TailReader lastTailReader;

    private long position = 0l;

    private Map<Long, TailReader> tailReaderMap = new LinkedHashMap<Long, TailReader>();

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

    public void addLastTailReader(TailReader lastTailReader) {
        this.lastTailReader = lastTailReader;
    }

    public void addFileTailerHandler(FileTailerHandler handler) {
        this.handlers.add(handler);
    }


    public void addTailReader(TailReader tailReader) {
        tailReaderMap.put(tailReader.getInode(), tailReader);
    }

    public Map<Long, TailReader> getTailReaderMap() {
        return tailReaderMap;
    }

    /**
     * get file inode for linux
     *
     * @param fileName
     * @return
     */
    private long getInode(String fileName) {
        try {
            return (long) Files.getAttribute(new File(fileName).toPath(), "unix:ino");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    /**
     * get file name by inode
     *
     * @param path
     * @param inode
     * @return
     */
    private String getFileNameByINode(String path, long inode) {
        File dir = new File(path);
        File[] files = dir.listFiles();
        for (File f : files) {
            long node = getInode(f.getAbsolutePath());
            if (node == inode) {
                return f.getAbsolutePath();
            }
        }
        return null;
    }

    /**
     * is a new file
     *
     * @param file
     * @param lastFileINode
     * @return
     */
    private boolean isFileNewer(String file, final long lastFileINode) {
        if (lastFileINode < 0) {
            return true;
        }
        long newFileINode = getInode(file);
        return !(newFileINode == lastFileINode);
    }

    /**
     * @param tailFileName
     * @return
     */
    private String getParentPath(String tailFileName) {
        File file = new File(tailFileName);
        return file.getParent();
    }

    public void run() {
        RandomAccessFile reader = null;

        long lastFileINode = -1;
        if (lastTailReader != null) { // get lastFileInode
            lastFileINode = lastTailReader.getInode();
        }

        boolean isFirst = true;

        String currentTailFileName = null;
        String line = null;

        String parentDir = getParentPath(tailFileName);

        long lineNum = 0;
        while (isRunning()) {
            while (reader == null) {
                try {
                    String lastTailFileName = null;
                    if (isFirst && lastFileINode > 0) {
                        lastTailFileName = getFileNameByINode(parentDir, lastFileINode);
                    }

                    if (!Strings.isNullOrEmpty(lastTailFileName)) {
                        currentTailFileName = lastTailFileName;
                        isFirst = false;
                    } else {
                        currentTailFileName = tailFileName;
                    }
                    reader = new RandomAccessFile(currentTailFileName, RAF_MODE);

                } catch (final FileNotFoundException e) {

                }
                if (reader == null) {
                    sleep(delayMillis);
                }
            }
            try {
                final boolean newer = isFileNewer(currentTailFileName, lastFileINode);

                if (newer) { // if is a new file
                    String lastTailFileName = getFileNameByINode(parentDir, lastFileINode);
                    if (!Strings.isNullOrEmpty(lastTailFileName)) {
                        tailReader = new TailReader(lastTailFileName, lastFileINode, new File(lastTailFileName).length());
                        tailReaderMap.put(lastFileINode, tailReader);
                    }
                    lineNum = 0l;
                    lastFileINode = getInode(currentTailFileName);
                    position = 0l; // a new file, set postition to 0

                    tailReader = new TailReader(currentTailFileName, lastFileINode, position);
                    tailReaderMap.put(lastFileINode, tailReader);
                    logger.info("tail a new file: " + currentTailFileName + ", inode: " + lastFileINode + ", pos: " + position);
                } else {
                    tailReader = new TailReader(currentTailFileName, lastFileINode, position);
                    tailReaderMap.put(lastFileINode, tailReader);
                }

                reader.seek(position);

                while ((line = reader.readLine()) != null) { // read file
                    line = new String(line.getBytes(charset), "UTF-8"); //编码转换
                    lineNum++;
                    position = reader.getFilePointer();
                    tailReader.setPosition(position); // update position
                    logger.info("tailReaderMap: " + tailReaderMap);
                    TailEvent event = new TailEvent(currentTailFileName, line, lineNum);
                    handle(event); // handle line
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

    private void handle(TailEvent event) {
        if (event != null && !Strings.isNullOrEmpty(event.getLine()) && handlers != null && !handlers.isEmpty()) {
            for (FileTailerHandler handler : handlers) {
                handler.process(event);
            }
        }
    }


    public TailReader getTailReader() {
        return tailReader;
    }

    public static class TailEvent implements Serializable {

        private String file;

        private String line;

        private long lineNum;

        public TailEvent(String file, String line, long lineNum) {
            this.file = file;
            this.line = line;
            this.lineNum = lineNum;
        }

        public String getFile() {
            return file;
        }

        public void setFile(String file) {
            this.file = file;
        }

        public String getLine() {
            return line;
        }

        public void setLine(String line) {
            this.line = line;
        }

        public long getLineNum() {
            return lineNum;
        }

        public void setLineNum(long lineNum) {
            this.lineNum = lineNum;
        }
    }
}

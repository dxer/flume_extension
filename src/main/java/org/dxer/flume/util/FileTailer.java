package org.dxer.flume.util;

import com.google.common.base.Strings;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
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
        this(tailFileName, null, DEFAULT_CHARSET, DEFAULT_DELAY_MILLIS);
    }

    public FileTailer(String tailFileName, long delayMillis) {
        this(tailFileName, null, DEFAULT_CHARSET, delayMillis);
    }

    public FileTailer(String tailFileName, TailReader lastTailReader, Charset charset, long delayMillis) {
        this.tailFileName = tailFileName;
        this.lastTailReader = lastTailReader;
        this.charset = charset;
        this.delayMillis = delayMillis;
    }

    public void addFileTailerHandler(FileTailerHandler handler) {
        this.handlers.add(handler);
    }


    public Map<Long, TailReader> getTailReaderMap() {
        return tailReaderMap;
    }

    /**
     * get file key
     *
     * @param file
     * @return
     */
    private String getFileKey(String file) {
        try {
            Path path = Paths.get(file);
            BasicFileAttributes bfa = Files.readAttributes(path, BasicFileAttributes.class);
            Object objectKey = bfa.fileKey();
            return objectKey.toString();
        } catch (Exception e) {
        }
        return null;
    }


    private long getInode(String fileName) {
        try {
            return (long) Files.getAttribute(new File(fileName).toPath(), "unix:ino");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

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

    private String getParentPath(String tailFileName) {
        File file = new File(tailFileName);
        return file.getParent();
    }

    public void run() {
        RandomAccessFile reader = null;

        long lastFileINode = -1;
        if (lastTailReader != null) {
            lastFileINode = lastTailReader.getInode();
        }

        boolean isFirst = true;

        long lineNum = 0;

        String currentTailFileName = null;
        String line = null;

        String parentDir = getParentPath(tailFileName);

        while (isRunning()) {
            while (reader == null) {
                try {
                    String lastTailFileName = null;
                    if (isFirst) {
                        lastTailFileName = getFileNameByINode(parentDir, lastFileINode);
                    }

                    if (!Strings.isNullOrEmpty(lastTailFileName)) { //
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
                    lineNum = 0l;
                    String lastTailFileName = getFileNameByINode(parentDir, lastFileINode);
                    if (!Strings.isNullOrEmpty(lastTailFileName)) {
                        tailReader = new TailReader(lastTailFileName, lastFileINode, new File(lastTailFileName).length());
                        tailReaderMap.put(lastFileINode, tailReader);
                    }

                    lastFileINode = getInode(currentTailFileName);
                    position = 0l; // a new file, set postition to 0

                    tailReader = new TailReader(currentTailFileName, lastFileINode, position);
                    tailReaderMap.put(lastFileINode, tailReader);
                    logger.info("tail a new file: " + currentTailFileName + ", inode: " + lastFileINode + ", pos: " + position + ", tailReaderMap: " + tailReaderMap);
                } else {
                    tailReader = new TailReader(currentTailFileName, lastFileINode, position);
                    tailReaderMap.put(lastFileINode, tailReader);
                }

                reader.seek(position);

                while ((line = reader.readLine()) != null) { // read file
                    line = new String(line.getBytes(charset), "UTF-8"); //编码转换
                    lineNum = lineNum + 1;
                    position = reader.getFilePointer();
                    tailReader.setPosition(position); // update position
                    logger.info("tailReaderMap: " + tailReaderMap);
                    handle(line); // handle line
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

    private void handle(String line) {
        if (!Strings.isNullOrEmpty(line) && handlers != null && !handlers.isEmpty()) {
            for (FileTailerHandler handler : handlers) {
                handler.process(line);
            }
        }
    }


    public TailReader getTailReader() {
        return tailReader;
    }

    public static void main(String[] args) {
        FileTailer tailer = new FileTailer("/home/hadoop/flume/logs/testlog.log", 500l);
        new Thread(tailer).start();
    }
}

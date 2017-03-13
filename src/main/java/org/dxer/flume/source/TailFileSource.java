package org.dxer.flume.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSourceConfigurationConstants;
import org.dxer.flume.util.FileTailer;
import org.dxer.flume.util.FlumeFileTailerHandler;
import org.dxer.flume.util.TailReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by linghf on 2017/2/6.
 */


public class TailFileSource extends AbstractSource implements EventDrivenSource,
        Configurable {

    private static final Logger logger = LoggerFactory.getLogger(TailFileSource.class);

    private String tailFileName;

    private String recordFileName;

    private Charset charset;

    private long delayMillis;

    private SourceCounter sourceCounter;

    private Integer bufferCount;

    private long batchTimeout;

    private Long lineMaxSize;

    private List<Event> eventList;

    private FileTailer tailer;

    private ScheduledExecutorService positionWriter;

    private ExecutorService tailFileService;

    private String positionFilePath;

    private Map<String, TailReader> tailReaderMap = new LinkedHashMap<String, TailReader>();

    private int writePosInitDelay = 5000;
    private int writePosInterval;


    @Override
    public synchronized void start() {
        TailReader tailReader = getLastTailReader(positionFilePath);
        tailer = new FileTailer(tailFileName, tailReader, charset, delayMillis);

        FlumeFileTailerHandler handler = new FlumeFileTailerHandler(getChannelProcessor(), sourceCounter, eventList, bufferCount,
                batchTimeout, charset);
        handler.setLineMaxSize(lineMaxSize); // line max size limit
        tailer.addFileTailerHandler(handler);

        tailFileService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("FileTail").build());
        tailFileService.execute(tailer);

        positionWriter = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("positionWriter").build());
        positionWriter.scheduleWithFixedDelay(new PositionWriterRunnable(),
                writePosInitDelay, 1000, TimeUnit.MILLISECONDS);

        sourceCounter.start();
        super.start();

        logger.debug("TailFileSource source started");
    }

    @Override
    public synchronized void stop() {
        super.stop();
        tailer.stop();
    }

    @Override
    public void configure(Context context) {
        tailFileName = context.getString(TailFileSourceConstant.TAIL_FILE);
        Preconditions.checkState(tailFileName != null, "The parameter tailFile must be specified");

        positionFilePath = context.getString(TailFileSourceConstant.POSITION_FILE, null);
        Preconditions.checkState(positionFilePath != null, "The parameter positionFile must be specified");

        charset = Charset.forName(context.getString(ExecSourceConfigurationConstants.CHARSET,
                ExecSourceConfigurationConstants.DEFAULT_CHARSET));
        delayMillis = context.getLong(TailFileSourceConstant.DELAY_MILLIS, 200l);

        bufferCount = context.getInteger(ExecSourceConfigurationConstants.CONFIG_BATCH_SIZE,
                ExecSourceConfigurationConstants.DEFAULT_BATCH_SIZE);

        batchTimeout = context.getLong(ExecSourceConfigurationConstants.CONFIG_BATCH_TIME_OUT,
                ExecSourceConfigurationConstants.DEFAULT_BATCH_TIME_OUT);

        lineMaxSize = context.getLong("lineMaxSize", null);

        eventList = new ArrayList<Event>();

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }


    /**
     * writePosition thread
     */
    private class PositionWriterRunnable implements Runnable {
        @Override
        public void run() {
            writePosition();
        }
    }

    private void writePosition() {
        File file = new File(positionFilePath);
        FileWriter writer = null;
        try {
            writer = new FileWriter(file);
            if (tailer != null && tailer.getTailReaderMap() != null) {
                List<TailReader> tailReaders = Lists.newArrayList();
                for (Long inode : tailer.getTailReaderMap().keySet()) {
                    tailReaders.add(tailer.getTailReaderMap().get(inode));
                }
                String json = new Gson().toJson(tailReaders);
                writer.write(json);
            }
        } catch (Throwable t) {
            logger.error("Failed writing positionFile", t);
        } finally {
            try {
                if (writer != null) writer.close();
            } catch (IOException e) {
                logger.error("Error: " + e.getMessage(), e);
            }
        }
    }

    /**
     * get last tail reader
     *
     * @param positionFilePath
     * @return
     */
    private TailReader getLastTailReader(String positionFilePath) {
        Long inode = null;
        Long position = null;
        FileReader fr = null;
        JsonReader jr = null;
        String file = null;
        TailReader tailReader = null;
        try {
            fr = new FileReader(positionFilePath);
            if (fr == null) {
                return null;
            }

            jr = new JsonReader(fr);
            if (jr == null) {
                return null;
            }
            jr.beginArray();

            while (jr.hasNext()) {
                inode = null;
                position = null;
                file = null;
                jr.beginObject();
                while (jr.hasNext()) {
                    switch (jr.nextName()) {
                        case "inode":
                            inode = jr.nextLong();
                            break;
                        case "pos":
                            position = jr.nextLong();
                            break;
                        case "file":
                            file = jr.nextString();
                            break;
                    }
                }
                jr.endObject();

                tailReader = new TailReader(file, inode, position);
            }

            jr.endArray();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tailReader;
    }
}




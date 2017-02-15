package org.dxer.flume.source;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSourceConfigurationConstants;
import org.dxer.flume.util.FileTailer;
import org.dxer.flume.util.FlumeFileTailerHandler;
import org.dxer.flume.util.PositionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

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

    private static final String SUFFIX_RECORD_LOG = ".index";

    private PositionManager manager;


    private void initFileTailer() {
        tailer = new FileTailer(tailFileName, charset, delayMillis);

        long startPosition = manager.getStartPosition();
        tailer.setStartPosition(startPosition);

        FlumeFileTailerHandler handler = new FlumeFileTailerHandler(getChannelProcessor(), sourceCounter, eventList, bufferCount,
                batchTimeout, charset);
        handler.setLineMaxSize(lineMaxSize); // line max size limit
        tailer.addFileTailerHandler(handler);
    }

    @Override
    public synchronized void start() {
        new Thread(tailer).start();

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
        tailFileName = context.getString("tailFile");
        Preconditions.checkState(tailFileName != null, "The parameter tailFile must be specified");

        recordFileName = context.getString("recordFile", null);
        charset = Charset.forName(context.getString(ExecSourceConfigurationConstants.CHARSET,
                ExecSourceConfigurationConstants.DEFAULT_CHARSET));
        delayMillis = context.getLong("delayMillis", 500l);

        bufferCount = context.getInteger(ExecSourceConfigurationConstants.CONFIG_BATCH_SIZE,
                ExecSourceConfigurationConstants.DEFAULT_BATCH_SIZE);

        batchTimeout = context.getLong(ExecSourceConfigurationConstants.CONFIG_BATCH_TIME_OUT,
                ExecSourceConfigurationConstants.DEFAULT_BATCH_TIME_OUT);

        lineMaxSize = context.getLong("lineMaxSize", null);

        eventList = new ArrayList<Event>();

        if (Strings.isNullOrEmpty(recordFileName)) {
            recordFileName = tailFileName + SUFFIX_RECORD_LOG;
        }

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }

        initFileTailer(); // init tailer
        manager = new PositionManager(recordFileName);
    }
}




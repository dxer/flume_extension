package org.dxer.flume.util;

import com.google.common.base.Strings;
import org.apache.flume.Event;
import org.apache.flume.SystemClock;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;


/**
 * Created by linghf on 2017/2/7.
 */

public class FlumeFileTailerHandler implements FileTailerHandler {

//    private static final Logger logger = LoggerFactory.getLogger(FlumeFileTailerHandler.class);

    private static final Logger logger = LoggerFactory.getLogger(FileTailer.class);

    private ChannelProcessor channelProcessor;
    private SourceCounter sourceCounter;
    private Charset charset;
    private SystemClock systemClock = new SystemClock();
    private Long lastPushToChannel = systemClock.currentTimeMillis();
    private List<Event> eventList;
    private final int bufferCount;
    private long batchTimeout;
    private ScheduledExecutorService timedFlushService;
    private ScheduledFuture<?> future;

    private Long lineMaxSize = null;

    public FlumeFileTailerHandler(ChannelProcessor channelProcessor, SourceCounter sourceCounter, List<Event> eventList, int bufferCount,
                                  long batchTimeout, Charset charset) {
        this.channelProcessor = channelProcessor;
        this.sourceCounter = sourceCounter;
        this.charset = charset;
        this.eventList = eventList;
        this.bufferCount = bufferCount;
        this.batchTimeout = batchTimeout;
    }

    public Long getLineMaxSize() {
        return lineMaxSize;
    }

    public void setLineMaxSize(Long lineMaxSize) {
        this.lineMaxSize = lineMaxSize;
    }

    @Override
    public void process(String line) {
        if (Strings.isNullOrEmpty(line)) {
            return;
        }

        if (lineMaxSize == null || (lineMaxSize != null && !Strings.isNullOrEmpty(line) && line.getBytes().length <= lineMaxSize.longValue())) {
            synchronized (eventList) {
                sourceCounter.incrementEventReceivedCount();
                eventList.add(EventBuilder.withBody(line.getBytes(charset)));
                if (eventList.size() >= bufferCount || timeout()) {
                    flushEventBatch(eventList);
                }
            }
        }
    }


    private void flushEventBatch(List<Event> eventList) {
        channelProcessor.processEventBatch(eventList);
        sourceCounter.addToEventAcceptedCount(eventList.size());
        eventList.clear();
        lastPushToChannel = systemClock.currentTimeMillis();
    }

    private boolean timeout() {
        return (systemClock.currentTimeMillis() - lastPushToChannel) >= batchTimeout;
    }
}

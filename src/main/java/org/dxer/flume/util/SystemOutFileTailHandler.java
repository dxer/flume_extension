package org.dxer.flume.util;

/**
 * Created by linghf on 2017/2/7.
 */

public class SystemOutFileTailHandler implements FileTailerHandler {
    @Override
    public void process(ReadEvent readEvent) {
        System.out.println(readEvent.getLine());
    }
}

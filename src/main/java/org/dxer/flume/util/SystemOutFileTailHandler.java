package org.dxer.flume.util;

/**
 * Created by linghf on 2017/2/7.
 */

public class SystemOutFileTailHandler implements FileTailerHandler {
    @Override
    public void process(FileTailer.TailEvent tailEvent) {
        if (tailEvent != null) {
            System.out.println(tailEvent.getLine());
        }
    }
}

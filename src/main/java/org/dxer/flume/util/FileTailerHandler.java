package org.dxer.flume.util;

/**
 * Created by linghf on 2017/2/7.
 */

public interface FileTailerHandler {

    public void process(FileTailer.TailEvent tailEvent);

}

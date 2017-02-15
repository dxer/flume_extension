package org.dxer.flume.util;


import org.apache.log4j.Logger;


/**
 * Created by linghf on 2017/2/8.
 */

public class WriteLog {

    private static final Logger logger = Logger.getLogger("WriteLog");

    /**
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {

        long index = 0;
        while (true) {
            logger.info(System.currentTimeMillis() + "\\t" + index);
            index++;
            Thread.sleep(1);
        }
    }
}

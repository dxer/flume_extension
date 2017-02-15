package org.dxer.flume.util;

import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by linghf on 2017/2/6.
 */

public class NioLineReader {
    /**
     * 换行符
     */
    public static final int LF = 10;

    /**
     * 回车符
     */
    public static final int CR = 13;


    public static void read(FileChannel fcin, ByteBuffer readBuffer, String encoding) {
        try {
            List<String> lineList = new ArrayList<String>();
            byte[] lineByte = null;

            byte[] temp = new byte[0];
            while (fcin.read(readBuffer) != -1) {
                int size = readBuffer.position();
                byte[] b = new byte[size];
                readBuffer.rewind();
                readBuffer.get(b);
                readBuffer.clear();

                int startNum = 0;
                boolean hasLF = false;//是否有换行符
                for (int i = 0; i < size; i++) {
                    if (b[i] == LF) {
                        hasLF = true;
                        int tempNum = temp.length;
                        int lineLen = i - startNum;
                        lineByte = new byte[tempNum + lineLen];
                        System.arraycopy(temp, 0, lineByte, 0, tempNum);//填充了lineByte[0]~lineByte[tempNum-1]
                        temp = new byte[0];
                        System.arraycopy(b, startNum, lineByte, tempNum, lineLen);//填充lineByte[tempNum]~lineByte[tempNum+lineNum-

                        String line = new String(lineByte, 0, lineByte.length, encoding);//一行完整的字符串(过滤了换行和回车)
                        lineList.add(line);

                        //过滤回车符和换行符
                        if (i + 1 < size && b[i + 1] == CR) {
                            startNum = i + 2;
                        } else {
                            startNum = i + 1;
                        }
                    }
                }
                if (hasLF) {
                    temp = new byte[b.length - startNum];
                    System.arraycopy(b, startNum, temp, 0, temp.length);
                } else { // 兼容单次读取的内容不足一行的情况
                    byte[] toTemp = new byte[temp.length + b.length];
                    System.arraycopy(temp, 0, toTemp, 0, temp.length);
                    System.arraycopy(b, 0, toTemp, temp.length, b.length);
                    temp = toTemp;
                }

            }
            if (temp != null && temp.length > 0) {//兼容文件最后一行没有换行的情况
                String line = new String(temp, 0, temp.length, encoding);
                lineList.add(line);
            }
            if (lineList != null && !lineList.isEmpty()) {

                for (String line : lineList) {
                    System.out.println(line);
                }
                System.out.println(lineList.size());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        FileInputStream fis = null;
        FileChannel channel = null;
        RandomAccessFile raf = null;
        try {
            fis = new FileInputStream("D:\\vv.txt");
            channel = fis.getChannel();
            ByteBuffer bf = ByteBuffer.allocate(1110);
            read(channel, bf, "UTF-8");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

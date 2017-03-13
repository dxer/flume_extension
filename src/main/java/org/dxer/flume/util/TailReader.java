package org.dxer.flume.util;

import java.io.Serializable;

/**
 * Created by linghf on 2017/2/15.
 */

public class TailReader implements Serializable {

    private String file;

    private long inode;


    private long position;

    public TailReader(String file, long inode, long position) {
        this.file = file;
        this.inode = inode;
        this.position = position;
    }

    public long getInode() {
        return inode;
    }

    public void setInode(long inode) {
        this.inode = inode;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return "file: " + file + ", inode: " + inode + ", pos: " + position;
    }
}

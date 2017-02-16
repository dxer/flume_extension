package org.dxer.flume.util;

import com.google.common.base.Strings;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Created by linghf on 2017/2/15.
 */

public class FileUtil {

    public static String getFileKey(String file) {
        try {
            Path path = Paths.get(file);
            BasicFileAttributes bfa = Files.readAttributes(path, BasicFileAttributes.class);
            Object objectKey = bfa.fileKey();
            return objectKey.toString();
        } catch (Exception e) {
        }
        return null;
    }

    public static boolean isFileNewer(String file, final String lastFileKey) {
        if (Strings.isNullOrEmpty(lastFileKey)) {
            return true;
        }
        String newFileKey = getFileKey(file);
        return !Strings.isNullOrEmpty(newFileKey) ? !newFileKey.equals(lastFileKey) : true;

    }
}

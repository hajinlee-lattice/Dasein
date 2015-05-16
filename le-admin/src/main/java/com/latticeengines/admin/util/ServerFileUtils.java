package com.latticeengines.admin.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.python.icu.impl.IllegalIcuArgumentException;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ServerFileUtils {

    private static boolean isValidPath(String path) {
        if (path == null || path.length() < 1) {
            IllegalArgumentException e =
                    new IllegalIcuArgumentException("Path cannot be empty.");
            throw new LedpException(LedpCode.LEDP_00003, e.getMessage(), e);
        }
        if (!path.startsWith("/")) {
            IllegalArgumentException e =
                    new IllegalIcuArgumentException("Path must start with /");
            throw new LedpException(LedpCode.LEDP_00003, e.getMessage(), e);
        }
        if (path.contains("/../") || path.startsWith("../") || path.endsWith("/..") || path.equals("..")) {
            IllegalArgumentException e =
                    new IllegalIcuArgumentException("Using \"..\" to access parent directory is forbidden.");
            throw new LedpException(LedpCode.LEDP_00003, e.getMessage(), e);
        }
        return true;
    }

    public static List<String> getFoldersAtPath(String path) {
        List<String> folderNames = new ArrayList<>();
        if (isValidPath(path))  {
            File dir = new File(path);
            if (dir.exists()) {
                File[] files = dir.listFiles();
                if (files != null) {
                    for (File file: files) {
                        if (file.isDirectory()) folderNames.add(file.getName());
                    }
                }
            }
        }
        return folderNames;
    }
}

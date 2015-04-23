package com.latticeengines.dellebi.util;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class NormalFileSystemOperations implements FileSystemOperations {

    private final static Logger LOGGER = Logger.getLogger(NormalFileSystemOperations.class);

    @Override
    public void cleanFolder(String folderName) {
        File folder = new File(folderName);
        try {
            FileUtils.cleanDirectory(folder);
        } catch (IOException e) {
            LOGGER.warn("Cannot clean folder!");
            LOGGER.warn("Failed!", e);
        }
    }

    @Override
    public int listFileNumber(String folderName) {
        int fileNumber = 0;
        File file = new File(folderName);
        FilenameFilter zipFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                String lowercaseName = name.toLowerCase();
                if (lowercaseName.endsWith(".txt")) {
                    LOGGER.info("Cascading found txt file " + lowercaseName + " and starts to process it.");
                    return true;
                } else {
                    return false;
                }
            }
        };
        fileNumber = file.listFiles(zipFilter).length;
        return fileNumber;

    }

    @Override
    public boolean isEmpty(String folderName) {
        int fileNumber = 0;
        File file = new File(folderName);
        FilenameFilter zipFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                String lowercaseName = name.toLowerCase();
                if (lowercaseName.endsWith(".txt")) {
                    return true;
                } else {
                    return false;
                }
            }
        };
        fileNumber = file.listFiles(zipFilter).length;
        if (fileNumber == 0) {
            return true;
        } else {
            return false;
        }

    }

}

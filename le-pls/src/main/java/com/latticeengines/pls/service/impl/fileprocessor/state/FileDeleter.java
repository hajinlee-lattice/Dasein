package com.latticeengines.pls.service.impl.fileprocessor.state;

import java.io.File;

import org.apache.commons.io.FileUtils;

public class FileDeleter {

    public void deleteFile(File file) {
        FileUtils.deleteQuietly(file);
    }
}

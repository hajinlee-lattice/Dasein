package com.latticeengines.pls.service.impl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;

public class CustomerSpaceHdfsFileDownloader extends AbstractHttpFileDownLoader {

    private Configuration yarnConfiguration;

    private String filePath;

    public CustomerSpaceHdfsFileDownloader(FileDownloadBuilder builder) {
        super(builder.mimeType);
        this.yarnConfiguration = builder.yarnConfiguration;
        this.filePath = builder.filePath;
    }

    @Override
    protected String getFileName() throws Exception {
        return StringUtils.substringAfterLast(filePath, "/");
    }

    @Override
    protected InputStream getFileInputStream() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        return fs.open(new Path(filePath));
    }

    public static class FileDownloadBuilder {

        private String mimeType;
        private Configuration yarnConfiguration;
        private String filePath;

        public FileDownloadBuilder setMimeType(String mimeType) {
            this.mimeType = mimeType;
            return this;
        }

        public FileDownloadBuilder setFilePath(String filePath) {
            this.filePath = filePath;
            return this;
        }

        public FileDownloadBuilder setYarnConfiguration(Configuration yarnConfiguration) {
            this.yarnConfiguration = yarnConfiguration;
            return this;
        }

    }
}

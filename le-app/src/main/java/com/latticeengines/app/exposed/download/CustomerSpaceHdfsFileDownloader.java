package com.latticeengines.app.exposed.download;

import java.io.InputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CustomerSpaceHdfsFileDownloader extends AbstractHttpFileDownLoader {

    private Configuration yarnConfiguration;

    private String filePath;

    private String fileName;

    public CustomerSpaceHdfsFileDownloader(FileDownloadBuilder builder) {
        super(builder.mimeType);
        this.yarnConfiguration = builder.yarnConfiguration;
        this.filePath = builder.filePath;
        this.fileName = builder.fileName;
    }

    @Override
    protected String getFileName() throws Exception {
        if (this.fileName != null) {
            return this.fileName;
        }
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
        private String fileName;

        public FileDownloadBuilder setMimeType(String mimeType) {
            this.mimeType = mimeType;
            return this;
        }

        public FileDownloadBuilder setFilePath(String filePath) {
            this.filePath = filePath;
            return this;
        }

        public FileDownloadBuilder setFileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public FileDownloadBuilder setYarnConfiguration(Configuration yarnConfiguration) {
            this.yarnConfiguration = yarnConfiguration;
            return this;
        }

    }
}

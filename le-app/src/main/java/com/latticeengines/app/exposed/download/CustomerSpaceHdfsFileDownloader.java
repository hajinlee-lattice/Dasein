package com.latticeengines.app.exposed.download;

import java.io.InputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;

public class CustomerSpaceHdfsFileDownloader extends AbstractHttpFileDownLoader {

    private Configuration yarnConfiguration;

    private String filePath;

    private String fileName;
    private String customer;

    public CustomerSpaceHdfsFileDownloader(FileDownloadBuilder builder) {
        super(builder.mimeType, builder.importFromS3Service, builder.cdlAttrConfigProxy, builder.batonService);
        this.yarnConfiguration = builder.yarnConfiguration;
        this.filePath = builder.filePath;
        this.fileName = builder.fileName;
        this.customer = builder.customer;
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
        String newFilePath = importFromS3Service.exploreS3FilePath(filePath);
        Path path = new Path(newFilePath);
        FileSystem fs = path.getFileSystem(yarnConfiguration);
        return fs.open(path);
    }

    public static class FileDownloadBuilder {

        private String mimeType;
        private Configuration yarnConfiguration;
        private String filePath;
        private String fileName;
        private String customer;
        private ImportFromS3Service importFromS3Service;
        private CDLAttrConfigProxy cdlAttrConfigProxy;
        private BatonService batonService;

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

        public FileDownloadBuilder setCustomer(String customer) {
            this.customer = customer;
            return this;
        }

        public FileDownloadBuilder setImportFromS3Service(ImportFromS3Service importFromS3Service) {
            this.importFromS3Service = importFromS3Service;
            return this;
        }

        public FileDownloadBuilder setCDLAttrConfigProxy(CDLAttrConfigProxy cdlAttrConfigProxy) {
            this.cdlAttrConfigProxy = cdlAttrConfigProxy;
            return this;
        }

        public FileDownloadBuilder setBatonService(BatonService batonService) {
            this.batonService = batonService;
            return this;
        }

    }
}

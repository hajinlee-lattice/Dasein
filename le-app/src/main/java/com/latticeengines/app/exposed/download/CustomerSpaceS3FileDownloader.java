package com.latticeengines.app.exposed.download;


import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;

public class CustomerSpaceS3FileDownloader extends AbstractHttpFileDownLoader {

    private static final Logger log = LoggerFactory.getLogger(CustomerSpaceS3FileDownloader.class);

    private String fileName;
    private String filePath;
    private String bucketName;

    public CustomerSpaceS3FileDownloader(S3FileDownloadBuilder builder) {
        super(builder.mimeType, builder.importFromS3Service, builder.cdlAttrConfigProxy, builder.batonService);
        this.fileName = builder.fileName;
        this.filePath = builder.filePath;
        this.bucketName = builder.bucketName;
    }

    @Override
    protected String getFileName() throws Exception {
        return this.fileName;
    }

    @Override
    protected InputStream getFileInputStream() throws Exception {
        return this.bucketName == null ? importFromS3Service.getS3FileInputStream(filePath)
                : importFromS3Service.getS3FileInputStream(bucketName, filePath);
    }

    public static class S3FileDownloadBuilder {

        private String mimeType;
        private String fileName;
        private String filePath;
        private String bucketName;
        private ImportFromS3Service importFromS3Service;
        private CDLAttrConfigProxy cdlAttrConfigProxy;
        private BatonService batonService;

        public S3FileDownloadBuilder setMimeType(String mimeType) {
            this.mimeType = mimeType;
            return this;
        }

        public S3FileDownloadBuilder setFileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public S3FileDownloadBuilder setFilePath(String filePath) {
            this.filePath = filePath;
            return this;
        }

        public S3FileDownloadBuilder setBucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public S3FileDownloadBuilder setImportFromS3Service(ImportFromS3Service importFromS3Service) {
            this.importFromS3Service = importFromS3Service;
            return this;
        }

        public S3FileDownloadBuilder setCDLAttrConfigProxy(CDLAttrConfigProxy cdlAttrConfigProxy) {
            this.cdlAttrConfigProxy = cdlAttrConfigProxy;
            return this;
        }

        public S3FileDownloadBuilder setBatonService(BatonService batonService) {
            this.batonService = batonService;
            return this;
        }
    }
}

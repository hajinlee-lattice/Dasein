package com.latticeengines.app.exposed.download;

import java.io.InputStream;

import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;

public class CustomerSpaceS3FileDownloader extends AbstractHttpFileDownLoader {

    private String fileName;
    private String filePath;

    public CustomerSpaceS3FileDownloader(S3FileDownloadBuilder builder) {
        super(builder.mimeType, builder.importFromS3Service, builder.cdlAttrConfigProxy, builder.batonService);
        this.fileName = builder.fileName;
        this.filePath = builder.filePath;
    }

    @Override
    protected String getFileName() throws Exception {
        return this.fileName;
    }

    @Override
    protected InputStream getFileInputStream() throws Exception {
        return importFromS3Service.getS3FileInputStream(filePath);
    }

    public static class S3FileDownloadBuilder {

        private String mimeType;
        private String fileName;
        private String filePath;
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

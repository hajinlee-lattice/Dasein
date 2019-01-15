package com.latticeengines.app.exposed.download;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import com.latticeengines.baton.exposed.service.BatonService;

public class DlFileHttpDownloader extends AbstractHttpFileDownLoader {

    private String fileName;
    private String fileContent;

    public DlFileHttpDownloader(String mimeType, String fileName, String fileContent) {
        super(mimeType, null, null, null);
        this.fileName = fileName;
        this.fileContent = fileContent;
    }

    public DlFileHttpDownloader(DlFileDownloaderBuilder builder) {
        super(builder.mimeType, null, null, builder.batonService);
        this.fileName = builder.fileName;
        this.fileContent = builder.fileContent;
    }

    @Override
    protected String getFileName() throws Exception {
        return fileName;
    }

    @Override
    protected InputStream getFileInputStream() throws Exception {
        InputStream stream = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8));
        return stream;
    }

    public static class DlFileDownloaderBuilder {
        private String mimeType;
        private String fileName;
        private String fileContent;
        private BatonService batonService;

        public DlFileDownloaderBuilder setMimeType(String mimeType) {
            this.mimeType = mimeType;
            return this;
        }

        public DlFileDownloaderBuilder setFileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public DlFileDownloaderBuilder setFileContent(String fileContent) {
            this.fileContent = fileContent;
            return this;
        }

        public DlFileDownloaderBuilder setBatonService(BatonService batonService) {
            this.batonService = batonService;
            return this;
        }
    }

}

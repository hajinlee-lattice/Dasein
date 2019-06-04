package com.latticeengines.pls.download;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import com.latticeengines.app.exposed.download.AbstractHttpFileDownLoader;
import com.latticeengines.baton.exposed.service.BatonService;

public class TemplateFileHttpDownloader extends AbstractHttpFileDownLoader {

    private String fileName;
    private String fileContent;

    public TemplateFileHttpDownloader(TemplateFileHttpDownloaderBuilder builder) {
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

    public static class TemplateFileHttpDownloaderBuilder {
        private String mimeType;
        private String fileName;
        private String fileContent;
        private BatonService batonService;

        public TemplateFileHttpDownloaderBuilder setMimeType(String mimeType) {
            this.mimeType = mimeType;
            return this;
        }

        public TemplateFileHttpDownloaderBuilder setFileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public TemplateFileHttpDownloaderBuilder setFileContent(String fileContent) {
            this.fileContent = fileContent;
            return this;
        }

        public TemplateFileHttpDownloaderBuilder setBatonService(BatonService batonService) {
            this.batonService = batonService;
            return this;
        }
    }

}

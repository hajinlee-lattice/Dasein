package com.latticeengines.app.exposed.download;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class DlFileHttpDownloader extends AbstractHttpFileDownLoader {

    private String fileName;
    private String fileContent;

    public DlFileHttpDownloader(String mimeType, String fileName, String fileContent) {
        super(mimeType, null, null, null);
        this.fileName = fileName;
        this.fileContent = fileContent;
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

}

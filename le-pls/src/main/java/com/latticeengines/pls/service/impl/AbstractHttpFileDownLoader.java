package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.FileCopyUtils;

import com.latticeengines.pls.service.HttpFileDownLoader;

public abstract class AbstractHttpFileDownLoader implements HttpFileDownLoader {

    private String mimeType;

    static final Log log = LogFactory.getLog(AbstractHttpFileDownLoader.class);

    protected abstract String getFileName();

    protected abstract InputStream getFileInputStream();

    protected AbstractHttpFileDownLoader(String mimeType) {
        this.mimeType = mimeType;
    }

    @Override
    public void downloadFile(HttpServletRequest request, HttpServletResponse response) throws IOException {

        response.setContentType(mimeType);
        response.setHeader("Content-Disposition", "attachment; filename=\"" + getFileName() + "\"");
        FileCopyUtils.copy(getFileInputStream(), response.getOutputStream());
    }

}

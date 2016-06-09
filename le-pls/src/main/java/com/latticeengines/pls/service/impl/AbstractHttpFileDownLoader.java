package com.latticeengines.pls.service.impl;

import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.FileCopyUtils;

import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.service.HttpFileDownLoader;

public abstract class AbstractHttpFileDownLoader implements HttpFileDownLoader {

    private String mimeType;
    private static final Log log = LogFactory.getLog(AbstractHttpFileDownLoader.class);

    protected abstract String getFileName() throws Exception;

    protected abstract InputStream getFileInputStream() throws Exception;

    protected AbstractHttpFileDownLoader(String mimeType) {
        this.mimeType = mimeType;
    }

    @Override
    public void downloadFile(HttpServletRequest request, HttpServletResponse response) {
        try {
            response.setContentType(mimeType);
            response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", getFileName()));
            if (mimeType.equals(MediaType.APPLICATION_OCTET_STREAM)) {
                try (InputStream is = getFileInputStream()) {
                    try (OutputStream os = response.getOutputStream()) {
                        GzipUtils.copyAndCompressStream(is, os);
                    }
                }
            } else {
                FileCopyUtils.copy(getFileInputStream(), response.getOutputStream());
            }

        } catch (Exception ex) {
            log.error("Failed to download file.", ex);
            throw new LedpException(LedpCode.LEDP_18022, ex);
        }
    }

}

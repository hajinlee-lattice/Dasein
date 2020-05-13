package com.latticeengines.app.exposed.service;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.domain.exposed.pls.FileDownloadConfig;

public interface FileDownloader<T extends FileDownloadConfig> {

    Class<T> configClz();

    void downloadByConfig(T downloadConfig, HttpServletRequest request, HttpServletResponse response) throws IOException;

}

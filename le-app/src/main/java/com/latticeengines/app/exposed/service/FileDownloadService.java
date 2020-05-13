package com.latticeengines.app.exposed.service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.domain.exposed.pls.FileDownloadConfig;


public interface FileDownloadService {

    String generateDownloadToken(FileDownloadConfig fileDownloadconfig);

    void downloadByToken(String token, HttpServletRequest request, HttpServletResponse response) throws Exception;

}

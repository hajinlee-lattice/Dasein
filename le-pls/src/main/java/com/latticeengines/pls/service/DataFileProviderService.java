package com.latticeengines.pls.service;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface DataFileProviderService {

    void downloadFile(HttpServletRequest request, HttpServletResponse response, String modelId, String mimeType,
            String filter) throws IOException;

    String getFileContents(String modelId, String mimeType, String filter) throws Exception;

}

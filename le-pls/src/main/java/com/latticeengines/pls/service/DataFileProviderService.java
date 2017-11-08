package com.latticeengines.pls.service;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface DataFileProviderService {

    void downloadFile(HttpServletRequest request, HttpServletResponse response, String modelId, String mimeType,
            String filter) throws IOException;

    void downloadPivotFile(HttpServletRequest request, HttpServletResponse response, String modelId, String mimeType)
            throws IOException;

    void downloadTrainingSet(HttpServletRequest request, HttpServletResponse response, String modelId, String mimeType)
            throws IOException;

    void downloadModelProfile(HttpServletRequest request, HttpServletResponse response, String modelId, String mimeType)
            throws IOException;

    void downloadFileByApplicationId(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String applicationId, String fileName) throws IOException;

    void downloadFileByFileName(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String fileName) throws IOException;

    void downloadFileByPath(HttpServletRequest request, HttpServletResponse response, String mimeType, String filePath)
            throws IOException;

    String getFileContents(String modelId, String mimeType, String filter) throws Exception;

}

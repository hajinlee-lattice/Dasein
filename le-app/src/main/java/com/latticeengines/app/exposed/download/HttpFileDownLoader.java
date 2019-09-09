package com.latticeengines.app.exposed.download;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface HttpFileDownLoader {

    void downloadFile(HttpServletRequest request, HttpServletResponse response);

    void downloadFile(HttpServletRequest request, HttpServletResponse response, DownloadMode mode);

    void downloadCsvWithTransform(HttpServletRequest request, HttpServletResponse response, Map<String, String> headerTransform);

    enum DownloadMode {
        TOP_PREDICTOR, //
        RF_MODEL, //
        DEFAULT
    }

}

package com.latticeengines.app.exposed.download;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface HttpFileDownLoader {

    void downloadFile(HttpServletRequest request, HttpServletResponse response);

    void downloadFile(HttpServletRequest request, HttpServletResponse response, DownloadMode mode);

    enum DownloadMode {
        TOP_PREDICTOR, //
        RF_MODEL, //
        DEFAULT
    }

}

package com.latticeengines.app.exposed.download;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface HttpFileDownLoader {

    void downloadFile(HttpServletRequest request, HttpServletResponse response);

}

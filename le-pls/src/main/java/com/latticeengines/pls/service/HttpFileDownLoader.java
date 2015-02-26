package com.latticeengines.pls.service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface HttpFileDownLoader {

    void downloadFile(HttpServletRequest request, HttpServletResponse response);

}

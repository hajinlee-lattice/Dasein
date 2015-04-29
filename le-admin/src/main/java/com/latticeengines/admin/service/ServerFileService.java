package com.latticeengines.admin.service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface ServerFileService {

    void downloadFile(HttpServletRequest request, HttpServletResponse response,
                      String path, String filename, String mimeType);

    String getRootPath();

}

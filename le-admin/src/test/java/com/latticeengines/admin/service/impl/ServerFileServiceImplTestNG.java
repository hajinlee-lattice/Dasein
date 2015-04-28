package com.latticeengines.admin.service.impl;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.service.ServerFileService;

public class ServerFileServiceImplTestNG extends AdminFunctionalTestNGBase {
    
    @Autowired
    private ServerFileService serverFileService;

    @Test(groups = "functional")
    public void getServiceFile() throws Exception {
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream os = mock(ServletOutputStream.class);

        String mimeType = "text/plain";
        String filename = "test.txt";
        String path = "/test/" + filename;

        try {
            when(response.getOutputStream()).thenReturn(os);
            serverFileService.downloadFile(request, response, path, filename, mimeType);
            verify(response).setHeader(eq("Content-Disposition"), anyString());
            verify(response).setContentType(mimeType);

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = "functional")
    public void getServiceFileWithImplicitFileName() throws Exception {
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream os = mock(ServletOutputStream.class);

        String mimeType = "text/plain";
        String path = "/test/test.txt";

        try {
            when(response.getOutputStream()).thenReturn(os);
            serverFileService.downloadFile(request, response, path, null, mimeType);
            verify(response).setHeader(eq("Content-Disposition"), anyString());
            verify(response).setContentType(mimeType);

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

}

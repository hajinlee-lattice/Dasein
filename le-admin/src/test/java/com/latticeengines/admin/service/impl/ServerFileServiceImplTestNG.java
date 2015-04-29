package com.latticeengines.admin.service.impl;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.service.ServerFileService;

public class ServerFileServiceImplTestNG extends AdminFunctionalTestNGBase {
    
    @Autowired
    private ServerFileService serverFileService;

    public String testFilePath = "/test/";
    public String testFileName = "test.txt";
    public String testFileType = "text/plain";
    public String testFileContent = "April 2015";

    @BeforeMethod(groups = "functional")
    public void createTestFile() throws IOException {
        String fullPath = serverFileService.getRootPath() + testFilePath;
        File dir = new File(fullPath);
        FileUtils.forceMkdir(dir);
        File file = new File(fullPath + testFileName);
        FileUtils.write(file, testFileContent);
    }

    @AfterMethod(groups = "functional")
    public void deleteTestFile() throws IOException {
        String fullPath = serverFileService.getRootPath() + testFilePath + testFileName;
        File file = new File(fullPath);
        FileUtils.deleteQuietly(file);
    }

    @Test(groups = "functional")
    public void getServiceFile() throws Exception {
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream os = mock(ServletOutputStream.class);

        String mimeType = testFileType;
        String filename = testFileName;
        String path = testFilePath + testFileName;

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

        String mimeType = testFileType;
        String path = testFilePath + testFileName;

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

package com.latticeengines.admin.util;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;

public class ServerFileUtilsTestNG extends AdminFunctionalTestNGBase {

    final private String parent = "parent";
    final private String[] folders = {"folder1", "folder2"};

    @Value("${admin.mount.rootpath}")
    private String rootPath;

    @BeforeMethod(groups = "functional")
    public void createTestFolders() throws IOException {
        for (String folder: folders) {
            String fullPath = rootPath + "/" + parent +  "/" + folder;
            File dir = new File(fullPath);
            FileUtils.forceMkdir(dir);
        }
    }

    @AfterMethod(groups = "functional")
    public void deleteTestFolders() throws IOException {
        for (String folder: folders) {
            String fullPath = rootPath + "/" + parent +  "/" + folder;
            File dir = new File(fullPath);
            FileUtils.deleteQuietly(dir);
        }
    }

    @Test(groups = "functional")
    public void getFolderList() throws Exception {
        List<String> result = ServerFileUtils.getFoldersAtPath(rootPath + "/" + parent);
        for (String folder: folders) {
            result.contains(folder);
        }
        Assert.assertEquals(result.size(), folders.length);
    }
}

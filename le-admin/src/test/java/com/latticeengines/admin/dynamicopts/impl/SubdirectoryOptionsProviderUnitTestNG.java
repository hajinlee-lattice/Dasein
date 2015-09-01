package com.latticeengines.admin.dynamicopts.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.dynamicopts.OptionsProvider;

public class SubdirectoryOptionsProviderUnitTestNG {

    final private String parent = "tmp";
    final private String[] folders = {"folder1", "folder2"};

    private String rootPath;
    private OptionsProvider provider;

    @BeforeMethod(groups = "unit")
    public void createTestFolders() throws IOException {
        File cwd = new File(".");
        rootPath = cwd.getCanonicalPath();
        Path parentDir = FileSystems.getDefault().getPath(rootPath, parent);
        FileUtils.forceMkdir(parentDir.toFile());
        for (String folder: folders) {
            Path dir = FileSystems.getDefault().getPath(rootPath, parent, folder);
            FileUtils.forceMkdir(dir.toFile());
        }
        Path path = FileSystems.getDefault().getPath(rootPath, parent);
        provider = new SubdirectoryOptionsProvider(path.toString());
    }

    @AfterMethod(groups = "unit", alwaysRun = true)
    public void deleteTestFolders() {
        Path parentDir = FileSystems.getDefault().getPath(rootPath, parent);
        FileUtils.deleteQuietly(parentDir.toFile());
    }

    @Test(groups = "unit")
    public void testSubdirectoryOptionsProvider() {
        List<String> options = provider.getOptions();
        Assert.assertEquals(options.size(), folders.length);
    }

    @Test(groups = "unit", timeOut = 10500L)
    public void testDirectoryWatch() throws IOException, InterruptedException {
        Path parentDir = FileSystems.getDefault().getPath(rootPath, parent);
        FileUtils.forceMkdir(parentDir.toFile());
        Path dir = FileSystems.getDefault().getPath(rootPath, parent, "folderX");
        FileUtils.forceMkdir(dir.toFile());
        int numOfRetries = 10;

        do {
            Thread.sleep(1000L);
            numOfRetries--;
        } while (numOfRetries > 0 && provider.getOptions().size() <= folders.length);

        Assert.assertEquals(provider.getOptions().size(), folders.length + 1);

        dir = FileSystems.getDefault().getPath(rootPath, parent, "folderX", "folderX1");
        FileUtils.forceMkdir(dir.toFile());
        Thread.sleep(3000L);
        Assert.assertEquals(provider.getOptions().size(), folders.length + 1);
    }


}

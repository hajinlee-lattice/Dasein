package com.latticeengines.admin.dynamicopts.impl;

import java.io.File;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.dynamicopts.OptionsProvider;

@Component
public class PermStoreProvider implements OptionsProvider {

    private MountedDirectoryOptionsProvider dirWatcher;

    private String absoluteRoot;

    @Value("${admin.mount.rootpath}")
    private String mountRoot;

    @Value("${admin.mount.vdb.permstore}")
    private String psPath;

    @PostConstruct
    private void watchDir() {
        if (dirWatcher == null) {
            Path path = FileSystems.getDefault().getPath(mountRoot, psPath);
            dirWatcher = new MountedDirectoryOptionsProvider(path);
        }
        absoluteRoot = dirWatcher.getAbsoluteRoot();
    }

    @Override
    public List<String> getOptions() {
        return dirWatcher.getOptions();
    }

    public String toRemoteAddr(String folder) {
        return dirWatcher.toRemoteAddr(folder);
    }

    public void createVDBFolder(String option, String visiDBServerName, String tenant) {
        Path absoluteRoot = FileSystems.getDefault().getPath(mountRoot, psPath);
        FileSystems.getDefault().getPath(absoluteRoot.toString(), option, visiDBServerName, tenant).toFile().mkdir();
    }

    public File getVDBFolder(String option, String server, String vdbName) {
        FileSystem fs = FileSystems.getDefault();
        return fs.getPath(absoluteRoot, option, server, vdbName).toFile();
    }

    public void deleteVDBFolder(String option, String server, String vdbName) {
        FileSystem fs = FileSystems.getDefault();
        Path path = fs.getPath(absoluteRoot, option, server, vdbName);
        FileUtils.deleteQuietly(path.toFile());
    }
}

package com.latticeengines.admin.dynamicopts.impl;

import java.io.File;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.dynamicopts.OptionsProvider;

@Component
public class DataStoreProvider implements OptionsProvider {

    private MountedDirectoryOptionsProvider dirWatcher;

    private String absoluteRoot;

    @Value("${admin.mount.rootpath}")
    private String mountRoot;

    @Value("${admin.mount.dl.datastore}")
    private String dsPath;

    @PostConstruct
    private void watchDir() {
        if (dirWatcher == null) {
            Path path = FileSystems.getDefault().getPath(mountRoot, dsPath);
            dirWatcher = new MountedDirectoryOptionsProvider(path);
        }
        absoluteRoot = dirWatcher.getAbsoluteRoot();
    }

    @Override
    public List<String> getOptions() {
        List<String> toReturn = new ArrayList<>();
        for (String option : dirWatcher.getOptions()) {
            toReturn.add(dirWatcher.toRemoteAddr(option));
        }
        return toReturn;
    }

    public String toRemoteAddr(String folder) {
        String addr = dirWatcher.toRemoteAddr(folder);
        if (addr != null) {
            return addr;
        } else {
            throw new RuntimeException(String.format("Option %s does not have a registered remote address", folder));
        }
    }

    public String toOptionKey(String remoteAddr) {
        return dirWatcher.toOptionKey(remoteAddr);
    }

    public void createTenantFolder(String server, String dmDeployment) {
        FileSystem fs = FileSystems.getDefault();
        fs.getPath(absoluteRoot, server, dmDeployment).toFile().mkdir();
        fs.getPath(absoluteRoot, server, dmDeployment, DLFolder.BACKUP.toPath()).toFile().mkdir();
        fs.getPath(absoluteRoot, server, dmDeployment, DLFolder.LAUNCH.toPath()).toFile().mkdir();
        fs.getPath(absoluteRoot, server, dmDeployment, DLFolder.STATUS.toPath()).toFile().mkdir();
    }

    public File getTenantFolder(String server, String dmDeployment) {
        FileSystem fs = FileSystems.getDefault();
        return fs.getPath(absoluteRoot, server, dmDeployment).toFile();
    }

    public void deleteTenantFolder(String server, String dmDeployment) {
        FileSystem fs = FileSystems.getDefault();
        FileUtils.deleteQuietly(fs.getPath(absoluteRoot, server, dmDeployment).toFile());
    }

    public enum DLFolder {
        BACKUP("Backup"), LAUNCH("Launch"), STATUS("Status");
        private final String path;

        DLFolder(String path) {
            this.path = path;
        }

        public String toPath() {
            return path;
        }
    }
}

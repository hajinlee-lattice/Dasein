package com.latticeengines.admin.dynamicopts.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

public class MountedDirectoryOptionsProvider extends SubdirectoryOptionsProvider {

    private Properties mountMap;
    private final String absoluteRoot;

    public MountedDirectoryOptionsProvider(Path path) {
        this(path, loadMountMap(path.toString()));
    }

    public MountedDirectoryOptionsProvider(Path path, Properties mountMap) {
        super(path);
        absoluteRoot = path.toAbsolutePath().toString();
        this.mountMap = mountMap;
    }

    public String toRemoteAddr(String key) {
        return mountMap.getProperty(key);
    }

    public String toOptionKey(String remoteAddr) {
        for (Map.Entry<Object, Object> entry : mountMap.entrySet()) {
            if ((entry.getValue()).equals(remoteAddr)) {
                return (String) entry.getKey();
            }
        }
        throw new RuntimeException(String.format("The remote address %s is not registered as an option key.",
                remoteAddr));
    }

    public String getAbsoluteRoot() {
        return this.absoluteRoot;
    }

    private static Properties loadMountMap(String path) {
        Properties props = new Properties();
        InputStream input = null;
        try {
            input = new FileInputStream(path + "/.mtable");
            // load mount table file
            props.load(input);
        } catch (IOException ex) {
            // ignore
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return props;
    }

}

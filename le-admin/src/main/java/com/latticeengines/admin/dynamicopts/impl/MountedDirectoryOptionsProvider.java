package com.latticeengines.admin.dynamicopts.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

public class MountedDirectoryOptionsProvider extends SubdirectoryOptionsProvider {

    private Properties mountMap;
    private final String defaultMntPnt;
    private final String absoluteRoot;

    public MountedDirectoryOptionsProvider(Path path, String defaultMntPnt) {
        this(path, defaultMntPnt, loadMountMap(path.toString()));
    }

    public MountedDirectoryOptionsProvider(Path path, String defaultMntPnt, Properties mountMap) {
        super(path);
        this.defaultMntPnt = defaultMntPnt;
        absoluteRoot = path.toAbsolutePath().toString();
        this.mountMap = mountMap;
    }

    public String toRemoteAddr(String key) { return mountMap.getProperty(key, defaultMntPnt); }

    public String toOptionKey(String remoteAddr) {
        for(Map.Entry<Object, Object> entry: mountMap.entrySet()){
            if ((entry.getValue()).equals(remoteAddr)) {
                return (String) entry.getKey();
            }
        }
        return null;
    }

    public String getAbsoluteRoot() { return this.absoluteRoot; }

    private static Properties loadMountMap(String path) {
        Properties props = new Properties();
        InputStream input = null;
        try {
            input = new FileInputStream(path + "/.mtable");
            // load mount table file
            props.load(new StringReader(IOUtils.toString(input).replace("\\", "\\\\")));
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

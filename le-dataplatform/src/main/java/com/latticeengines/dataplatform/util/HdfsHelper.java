package com.latticeengines.dataplatform.util;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.util.StreamUtils;

public class HdfsHelper {

    public static interface HdfsFilenameFilter {
        boolean accept(Path filename);
    }

    public static enum LogFileEncodingType {
        NONE, GZ;

        public static LogFileEncodingType getEnum(String s) {
            if (NONE.name().equalsIgnoreCase(s)) {
                return NONE;
            } else if (GZ.name().equalsIgnoreCase(s)) {
                return GZ;
            }
            throw new IllegalArgumentException("No Enum specified for this string");
        }
    };

    public static final void mkdir(Configuration configuration, String dir) throws Exception {
        FileSystem fs = FileSystem.get(configuration);
        fs.mkdirs(new Path(dir));
    }

    public static final void rmdir(Configuration configuration, String dir) throws Exception {
        FileSystem fs = FileSystem.get(configuration);
        fs.delete(new Path(dir), true);
    }

    public static final String getHdfsFileContents(Configuration configuration, String hdfsPath) throws Exception {
        FileSystem fs = FileSystem.get(configuration);
        Path schemaPath = new Path(hdfsPath);
        InputStream is = fs.open(schemaPath);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        StreamUtils.copy(is, os);
        return new String(os.toByteArray());
    }

    public static final void writeToFile(Configuration configuration, String hdfsPath, String contents)
            throws Exception {
        FileSystem fs = FileSystem.get(configuration);
        Path schemaPath = new Path(hdfsPath);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(schemaPath, true)));
        try {
            br.write(contents);
        } finally {
            br.close();
        }
    }

    public static final List<String> getFilesForDir(Configuration configuration, String hdfsDir) throws Exception {
        return getFilesForDir(configuration, hdfsDir, null);
    }

    public static final List<String> getFilesForDir(Configuration configuration, String hdfsDir,
            HdfsFilenameFilter filter) throws Exception {
        FileSystem fs = FileSystem.get(configuration);
        FileStatus[] statuses = fs.listStatus(new Path(hdfsDir));
        List<String> filePaths = new ArrayList<String>();
        for (FileStatus status : statuses) {
            Path filename = status.getPath();
            boolean accept = true;

            if (filter != null) {
                accept = filter.accept(filename);
            }
            if (accept) {
                filePaths.add(filename.toString());
            }
        }

        return filePaths;
    }

    public static final String getApplicationLog(Configuration configuration, String user, String applicationId)
            throws Exception {
        FileSystem fs = FileSystem.get(configuration);
        String hdfsPath = configuration.get("yarn.nodemanager.remote-app-log-dir") + "/" + user + "/logs/"
                + applicationId;
        String encoding = configuration.get("yarn.nodemanager.log-aggregation.compression-type");
        Path schemaPath = new Path(hdfsPath);
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(schemaPath, false);
        String log = "";
        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            Path filePath = file.getPath();
            InputStream is = null;
            switch (LogFileEncodingType.valueOf(encoding.toUpperCase())) {
            case NONE:
                is = fs.open(filePath);
                break;
            case GZ:
                is = new GZIPInputStream(fs.open(filePath));
                break;
            }
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            StreamUtils.copy(is, os);
            log = log.concat(new String(os.toByteArray()));
        }
        return log;
    }

}

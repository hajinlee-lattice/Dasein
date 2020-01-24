package com.latticeengines.common.exposed.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.springframework.util.StreamUtils;

public final class WebHdfsUtils {

    protected WebHdfsUtils() {
        throw new UnsupportedOperationException();
    }

    public static final void mkdir(String uri, Configuration configuration, String dir) throws IOException {
        try (WebHdfsFileSystem fs = new WebHdfsFileSystem()) {
            fs.initialize(new Path(uri).toUri(), configuration);
            fs.mkdirs(new Path(dir));
        }
    }

    public static final boolean isDirectory(String uri, Configuration configuration, String dir)
            throws IllegalArgumentException, IOException {
        try (WebHdfsFileSystem fs = new WebHdfsFileSystem()) {
            fs.initialize(new Path(uri).toUri(), configuration);
            return fs.isDirectory(new Path(dir));
        }
    }

    public static final void writeToFile(String uri, Configuration configuration, String dir, String contents)
            throws IllegalArgumentException, IOException {
        try (WebHdfsFileSystem fs = new WebHdfsFileSystem()) {
            fs.initialize(new Path(uri).toUri(), configuration);
            FSDataOutputStream out = fs.create(new Path(dir));
            out.writeBytes(contents);
            out.close();
        }
    }

    public static final String getWebHdfsFileContents(String uri, Configuration configuration, String dir)
            throws IllegalArgumentException, IOException {
        try (WebHdfsFileSystem fs = new WebHdfsFileSystem()) {
            fs.initialize(new Path(uri).toUri(), configuration);
            try (InputStream is = fs.open(new Path(dir))) {
                try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                    StreamUtils.copy(is, os);
                    return new String(os.toByteArray());
                }
            }
        }
    }

    public static final FileStatus getFileStatus(String uri, Configuration configuration, String dir)
            throws IllegalArgumentException, IOException {
        try (WebHdfsFileSystem fs = new WebHdfsFileSystem()) {
            fs.initialize(new Path(uri).toUri(), configuration);
            return fs.getFileStatus(new Path(dir));
        }
    }

    public static final void appendFileContents(String uri, Configuration configuration, String dir, String contents)
            throws IllegalArgumentException, IOException {
        try (WebHdfsFileSystem fs = new WebHdfsFileSystem()) {
            fs.initialize(new Path(uri).toUri(), configuration);
            FSDataOutputStream out = fs.append(new Path(dir));
            out.writeBytes(contents);
            out.close();
        }
    }

    public static final void updateDirectoryName(String uri, Configuration configuration, String sourceDir,
            String destinationDir) throws IllegalArgumentException, IOException {
        try (WebHdfsFileSystem fs = new WebHdfsFileSystem()) {
            fs.initialize(new Path(uri).toUri(), configuration);
            fs.rename(new Path(sourceDir), new Path(destinationDir));
        }
    }

    public static final void rmdir(String uri, Configuration configuration, String dir)
            throws IllegalArgumentException, IOException {
        try (WebHdfsFileSystem fs = new WebHdfsFileSystem()) {
            fs.initialize(new Path(uri).toUri(), configuration);
            fs.delete(new Path(dir), true);
        }
    }
}

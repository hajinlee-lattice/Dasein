package com.latticeengines.common.exposed.util;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.util.StreamUtils;

public class HdfsUtils {

    public interface HdfsFileFormat {
        public static final String AVRO_FILE = ".*.avro";
        public static final String AVSC_FILE = ".*.avsc";
        public static final String JSON_FILE = ".*.json";
    }

    public interface HdfsFilenameFilter {
        boolean accept(String filename);
    }

    public interface HdfsFileFilter {
        boolean accept(FileStatus file);
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
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            fs.mkdirs(new Path(dir));
        }
    }

    public static final void rmdir(Configuration configuration, String dir) throws Exception {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            fs.delete(new Path(dir), true);
        }
    }

    public static final String getHdfsFileContents(Configuration configuration, String hdfsPath) throws Exception {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            Path schemaPath = new Path(hdfsPath);

            try (InputStream is = fs.open(schemaPath)) {
                try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                    StreamUtils.copy(is, os);
                    return new String(os.toByteArray());
                }
            }
        }
    }
    
    public static final void copyInputStreamToHdfs(Configuration configuration, InputStream inputStream, String hdfsPath) 
            throws Exception{
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            try (OutputStream outputStream = fs.create(new Path(hdfsPath))) {
                IOUtils.copy(inputStream, outputStream);
            }
        }
    }

    public static final void copyLocalToHdfs(Configuration configuration, String localPath, String hdfsPath)
            throws Exception {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            fs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
        }
    }

    public static final void copyHdfsToLocal(Configuration configuration, String hdfsPath, String localPath)
            throws Exception {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            fs.copyToLocalFile(new Path(hdfsPath), new Path(localPath));
        }
    }

    public static final void writeToFile(Configuration configuration, String hdfsPath, String contents)
            throws Exception {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            Path filePath = new Path(hdfsPath);

            try (BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(filePath, true)))) {
                br.write(contents);
            }
        }
    }

    public static final boolean fileExists(Configuration configuration, String hdfsPath) throws Exception {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            return fs.exists(new Path(hdfsPath));
        }
    }

    public static final List<String> getFilesForDir(Configuration configuration, String hdfsDir) throws Exception {
        return getFilesForDir(configuration, hdfsDir, (HdfsFilenameFilter) null);
    }

    public static final List<String> getFilesForDir(Configuration configuration, String hdfsDir, final String regex)
            throws Exception {
        HdfsFilenameFilter filter = new HdfsFilenameFilter() {

            @Override
            public boolean accept(String filename) {
                Pattern p = Pattern.compile(regex);
                Matcher matcher = p.matcher(filename.toString());
                return matcher.matches();
            }
        };

        return getFilesForDir(configuration, hdfsDir, filter);
    }

    public static final List<String> getFilesForDir(Configuration configuration, String hdfsDir,
            HdfsFilenameFilter filter) throws Exception {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            FileStatus[] statuses = fs.listStatus(new Path(hdfsDir));
            List<String> filePaths = new ArrayList<String>();
            for (FileStatus status : statuses) {
                Path filePath = status.getPath();
                boolean accept = true;

                if (filter != null) {
                    accept = filter.accept(filePath.getName());
                }
                if (accept) {
                    filePaths.add(filePath.toString());
                }
            }

            return filePaths;
        }
    }

    public static final List<String> getFilesForDir(Configuration configuration, String hdfsDir, HdfsFileFilter filter)
            throws Exception {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            FileStatus[] statuses = fs.listStatus(new Path(hdfsDir));
            List<String> filePaths = new ArrayList<String>();
            for (FileStatus status : statuses) {
                Path filePath = status.getPath();
                boolean accept = true;

                if (filter != null) {
                    accept = filter.accept(status);
                }
                if (accept) {
                    filePaths.add(filePath.toString());
                }
            }

            return filePaths;
        }
    }

    public static final List<String> getFilesForDirRecursive(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter) throws Exception {
        return getFilesForDirRecursive(configuration, hdfsDir, filter, false);
    }

    public static final List<String> getFilesForDirRecursive(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter, boolean returnFirstMatch) throws Exception {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            FileStatus[] statuses = fs.listStatus(new Path(hdfsDir));
            Set<String> filePaths = new HashSet<String>();
            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    filePaths.addAll(getFilesForDir(configuration, status.getPath().toString(), filter));
                    if (returnFirstMatch && filePaths.size() > 0) {
                        break;
                    }
                    filePaths.addAll(getFilesForDirRecursive(configuration, status.getPath().toString(), filter));
                }
            }
            return new ArrayList<>(filePaths);
        }
    }

    public static final String getApplicationLog(Configuration configuration, String user, String applicationId)
            throws Exception {
        String log = "";
        try (InputStream is = getInputStream(configuration, user, applicationId)) {
            try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                StreamUtils.copy(is, os);
                log = log.concat(new String(os.toByteArray()));
            } catch (IOException e1) {
                throw new RuntimeException(e1);
            }
        } catch (IOException e2) {
            throw new RuntimeException(e2);
        }
        return log;
    }

    private static InputStream getInputStream(Configuration configuration, String user, String applicationId)
            throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            String hdfsPath = configuration.get("yarn.nodemanager.remote-app-log-dir") + "/" + user + "/logs/"
                    + applicationId;
            String encoding = configuration.get("yarn.nodemanager.log-aggregation.compression-type");
            Path schemaPath = new Path(hdfsPath);
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(schemaPath, false);
            InputStream is = null;
            while (iterator.hasNext()) {
                LocatedFileStatus file = iterator.next();
                Path filePath = file.getPath();
                switch (LogFileEncodingType.valueOf(encoding.toUpperCase())) {
                case NONE:
                    is = fs.open(filePath);
                    break;
                case GZ:
                    is = new GZIPInputStream(fs.open(filePath));
                    break;
                }
            }
            return is;
        }
    }

    public static List<String> getFilesByGlob(Configuration configuration, String globPath) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            FileStatus[] statuses = fs.globStatus(new Path(globPath));
            List<String> filePaths = new ArrayList<>();
            if (statuses == null) {
                return filePaths;
            }
            for (FileStatus status : statuses) {
                Path filePath = status.getPath();
                filePaths.add(Path.getPathWithoutSchemeAndAuthority(filePath).toString());
            }
            return filePaths;
        }
    }

    public static boolean moveFile(Configuration configuration, String src, String dst) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            return fs.rename(new Path(src), new Path(dst));
        }
    }

    public static boolean copyFiles(Configuration configuration, String src, String dst)
            throws IllegalArgumentException, IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            return FileUtil.copy(fs, new Path(src), fs, new Path(dst), false, false, configuration);
        }
    }

    public static FileChecksum getCheckSum(Configuration configuration, String path) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            return fs.getFileChecksum(new Path(path));
        }
    }
}

package com.latticeengines.common.exposed.util;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.StreamUtils;

public class HdfsUtils {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(HdfsUtils.class);
    private static final int EOF = -1;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

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

    public static final void mkdir(Configuration configuration, String dir) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            fs.mkdirs(new Path(dir));
        }
    }

    public static final boolean isDirectory(Configuration configuration, String path) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            return fs.isDirectory(new Path(path));
        }
    }

    public static final void rmdir(Configuration configuration, String dir) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            fs.delete(new Path(dir), true);
        }
    }

    public static final String getHdfsFileContents(Configuration configuration, String hdfsPath) throws IOException {
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

    public static final void copyInputStreamToHdfs(Configuration configuration, InputStream inputStream,
            String hdfsPath) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            try (OutputStream outputStream = fs.create(new Path(hdfsPath))) {
                IOUtils.copy(inputStream, outputStream);
            }
        }
    }

    public static final void copyInputStreamToDest(URI scheme, Configuration configuration, InputStream inputStream)
            throws IOException {
        try (FileSystem fs = FileSystem.newInstance(scheme, configuration)) {
            try (OutputStream outputStream = fs.create(new Path(scheme.getPath()))) {
                IOUtils.copy(inputStream, outputStream);
            }
        }
    }

    public static final void copyInputStreamToHdfsWithoutBom(Configuration configuration, InputStream inputStream,
            String hdfsPath) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            try (OutputStream outputStream = fs.create(new Path(hdfsPath))) {
                IOUtils.copy(new BOMInputStream(inputStream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                        ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE), outputStream);
            }
        }
    }

    public static FileSystem getFileSystem(Configuration configuration) throws IOException {
        return FileSystem.newInstance(configuration);
    }

    public static final void copyLocalResourceToHdfs(Configuration configuration, String resourcePath, String hdfsPath)
            throws IOException {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource resource = resolver.getResource(resourcePath);
        copyLocalToHdfs(configuration, resource.getFile().getAbsolutePath(), hdfsPath);
    }

    public static final void copyLocalToHdfs(Configuration configuration, String localPath, String hdfsPath)
            throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            fs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
        }
    }

    public static final void copyFromLocalDirToHdfs(Configuration configuration, String localPath, String hdfsPath)
            throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            FileUtil.copy(new File(localPath), fs, new Path(hdfsPath), false, configuration);
        }
    }

    public static final void copyHdfsToLocal(Configuration configuration, String hdfsPath, String localPath)
            throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            fs.copyToLocalFile(new Path(hdfsPath), new Path(localPath));
        }
    }

    public static final void writeToFile(Configuration configuration, String hdfsPath, String contents)
            throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            Path filePath = new Path(hdfsPath);

            try (BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(filePath, true)))) {
                br.write(contents);
            }
        }
    }

    public static final void uncompressGZFileWithinHDFS(Configuration configuration, String gzHdfsPath,
            String uncompressedFilePath) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            Path inputFilePath = new Path(gzHdfsPath);
            Path outputFilePath = new Path(uncompressedFilePath);
            try (InputStream is = new GZIPInputStream(fs.open(inputFilePath))) {
                OutputStream os = fs.create(outputFilePath, true);
                org.apache.hadoop.io.IOUtils.copyBytes(is, os, configuration);
            }
        }
    }

    public static final void uncompressZipFileWithinHDFS(Configuration configuration, String compressedFile,
            String uncompressedDir) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            Path inputFile = new Path(compressedFile);
            Path outputFolder = new Path(uncompressedDir);
            try (ZipInputStream is = new ZipInputStream(fs.open(inputFile))) {
                ZipEntry entry = is.getNextEntry();
                while (entry != null) {
                    if (entry.isDirectory()) {
                        entry = is.getNextEntry();
                        continue;
                    }
                    Path outputFile = new Path(outputFolder, entry.getName());
                    OutputStream os = fs.create(outputFile, true);
                    org.apache.hadoop.io.IOUtils.copyBytes(is, os, configuration, false);
                    os.close();
                    entry = is.getNextEntry();
                }
            }
        }
    }

    public static final void compressGZFileWithinHDFS(Configuration configuration, String gzHdfsPath,
            String uncompressedFilePath) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            Path inputFilePath = new Path(uncompressedFilePath);
            Path outputFilePath = new Path(gzHdfsPath);
            try (OutputStream os = new GZIPOutputStream(fs.create(outputFilePath), true)) {
                InputStream is = fs.open(inputFilePath);
                org.apache.hadoop.io.IOUtils.copyBytes(is, os, configuration);
            }
        }
    }

    public static final boolean fileExists(Configuration configuration, String hdfsPath) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            return fs.exists(new Path(hdfsPath));
        }
    }

    public static final List<String> getFilesForDir(Configuration configuration, String hdfsDir) throws IOException {
        return getFilesForDir(configuration, hdfsDir, (HdfsFilenameFilter) null);
    }

    public static final List<String> getFilesForDir(Configuration configuration, String hdfsDir, final String regex)
            throws IOException {
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
            HdfsFilenameFilter filter) throws IOException {
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
            throws IOException {
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

    // Only return files. Exclude all the sub directory paths
    public static final List<String> onlyGetFilesForDir(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            FileStatus[] statuses = fs.listStatus(new Path(hdfsDir));
            List<String> filePaths = new ArrayList<String>();
            for (FileStatus status : statuses) {
                if (!status.isDirectory()) {
                    Path filePath = status.getPath();
                    boolean accept = true;
                    if (filter != null) {
                        accept = filter.accept(status);
                    }
                    if (accept) {
                        filePaths.add(filePath.toString());
                    }
                }
            }
            return filePaths;
        }
    }

    public static final List<FileStatus> getFileStatusesForDir(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            FileStatus[] statuses = fs.listStatus(new Path(hdfsDir));
            List<FileStatus> filePaths = new ArrayList<>();
            for (FileStatus status : statuses) {
                boolean accept = true;

                if (filter != null) {
                    accept = filter.accept(status);
                }
                if (accept) {
                    filePaths.add(status);
                }
            }

            return filePaths;
        }
    }

    public static final List<String> getFilesForDirRecursive(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter) throws IOException {
        return getFilesForDirRecursive(configuration, hdfsDir, filter, false);
    }

    public static final List<String> getFilesForDirRecursive(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter, boolean returnFirstMatch) throws IOException {
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

    public static final List<String> getFilesForDirRecursiveWithFilterOnDir(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter, HdfsFileFilter folderFilter) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            FileStatus[] statuses = fs.listStatus(new Path(hdfsDir));
            Set<String> filePaths = new HashSet<String>();
            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    if (folderFilter.accept(status)) {
                        filePaths.addAll(getFilesForDir(configuration, status.getPath().toString(), filter));
                        filePaths.addAll(getFilesForDirRecursiveWithFilterOnDir(configuration,
                                status.getPath().toString(), filter, folderFilter));
                    }
                }
            }
            return new ArrayList<>(filePaths);
        }
    }

    // Only return files. Exclude all the sub directory paths
    public static final List<String> onlyGetFilesForDirRecursive(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter, boolean returnFirstMatch) throws IOException {
        Set<String> filePaths = new HashSet<String>();
        onlyGetFilesForDirRecursiveHelper(configuration, hdfsDir, filter, returnFirstMatch, filePaths);
        return new ArrayList<>(filePaths);
    }

    public static final void onlyGetFilesForDirRecursiveHelper(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter, boolean returnFirstMatch, Set<String> filePaths) throws IOException {
        if (returnFirstMatch && filePaths.size() > 0) {
            return;
        }
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            FileStatus[] statuses = fs.listStatus(new Path(hdfsDir));
            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    onlyGetFilesForDirRecursiveHelper(configuration, status.getPath().toString(), filter,
                            returnFirstMatch, filePaths);
                } else {
                    boolean accept = true;
                    if (filter != null) {
                        accept = filter.accept(status);
                    }
                    if (accept) {
                        filePaths.add(status.getPath().toString());
                    }
                }
                if (returnFirstMatch && filePaths.size() > 0) {
                    return;
                }
            }
        }
    }

    public static final List<FileStatus> getFileStatusesForDirRecursive(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter) throws IOException {
        return getFileStatusesForDirRecursive(configuration, hdfsDir, filter, false);
    }

    public static final List<FileStatus> getFileStatusesForDirRecursive(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter, boolean returnFirstMatch) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            FileStatus[] statuses = fs.listStatus(new Path(hdfsDir));
            Set<FileStatus> filePaths = new HashSet<>();
            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    filePaths.addAll(getFileStatusesForDir(configuration, status.getPath().toString(), filter));
                    if (returnFirstMatch && filePaths.size() > 0) {
                        break;
                    }
                    filePaths
                            .addAll(getFileStatusesForDirRecursive(configuration, status.getPath().toString(), filter));
                }
            }
            return new ArrayList<>(filePaths);
        }
    }

    public static final String getApplicationLog(Configuration configuration, String user, String applicationId)
            throws IOException {
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

    public static InputStream getInputStream(Configuration configuration, String hdfsPath) throws IOException {
        FileSystem fs = FileSystem.newInstance(configuration);
        return fs.open(new Path(hdfsPath));
    }

    public static void copyFromLocalToHdfs(Configuration configuration, String localPath, String hdfsPath)
            throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            fs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
        }
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
        EncryptionZone dstZone = getEncryptionZone(configuration, dst);
        if (dstZone != null) {
            EncryptionZone srcZone = getEncryptionZone(configuration, src);
            if (srcZone == null || !keyEquals(srcZone, dstZone)) {
                String dstKey = "Key = " + dstZone.getKeyName();
                String srcKey = srcZone == null ? "No Key" : srcZone.getKeyName();
                log.info(String.format(
                        "Destination (%s) is encrypted differently than source (%s). Use copy and rm instead of mv.",
                        dstKey, srcKey));
                if (copyFiles(configuration, src, dst)) {
                    rmdir(configuration, src);
                    return true;
                } else {
                    return false;
                }
            }
        }
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            return fs.rename(new Path(src), new Path(dst));
        }
    }

    private static boolean keyEquals(EncryptionZone zone1, EncryptionZone zone2) {
        return zone1.getKeyName().equals(zone2.getKeyName()) && zone1.getVersion().equals(zone2.getVersion());
    }

    public static boolean copyFiles(Configuration configuration, String src, String dst)
            throws IllegalArgumentException, IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            return FileUtil.copy(fs, new Path(src), fs, new Path(dst), false, false, configuration);
        }
    }

    public static final void copyGlobToDir(Configuration configuration, String sourceGlob, String targetDir)
            throws IOException {
        for (String filePath : getFilesByGlob(configuration, sourceGlob)) {
            if (!isDirectory(configuration, targetDir)) {
                mkdir(configuration, targetDir);
            }
            String fileName = new Path(filePath).getName();
            copyFiles(configuration, filePath, new Path(targetDir, fileName).toString());
        }
    }

    public static FileChecksum getCheckSum(Configuration configuration, String path) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            return fs.getFileChecksum(new Path(path));
        }
    }

    public static Long getFileSize(Configuration configuration, String filePath) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            FileStatus status = fs.getFileStatus(new Path(filePath));
            return status.getLen();
        }
    }

    public static List<EncryptionZone> getEncryptionZones(Configuration configuration) {
        try (DistributedFileSystem fs = (DistributedFileSystem) DistributedFileSystem.newInstance(configuration)) {
            List<EncryptionZone> zones = new ArrayList<>();
            RemoteIterator<EncryptionZone> iter = fs.listEncryptionZones();
            while (iter.hasNext()) {
                zones.add(iter.next());
            }
            return zones;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void createEncryptionZone(Configuration configuration, String path, String keyname) {
        try (DistributedFileSystem fs = (DistributedFileSystem) DistributedFileSystem.newInstance(configuration)) {
            fs.createEncryptionZone(new Path(path), keyname);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isEncryptionZone(Configuration configuration, String path) {
        return getEncryptionZone(configuration, path) != null;
    }

    private static EncryptionZone getEncryptionZone(Configuration configuration, String path) {
        List<EncryptionZone> zones = getEncryptionZones(configuration);
        for (EncryptionZone zone : zones) {
            if (path.startsWith(zone.getPath())) {
                return zone;
            }
        }
        return null;
    }

    public static void deleteKey(Configuration configuration, String keyName) {
        KeyProvider provider = getKeyProvider(configuration);
        try {
            provider.deleteKey(keyName);
            provider.flush();
        } catch (IOException e) {
            throw new RuntimeException(String.format("Could not delete key %s with provider %s", keyName, provider), e);
        }
    }

    public static void createKey(Configuration configuration, String keyName) {
        KeyProvider provider = getKeyProvider(configuration);
        try {
            provider.createKey(keyName, new KeyProvider.Options(configuration));
            provider.flush();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not create key %s with provider %s", keyName, provider), e);
        }
    }

    public static void rollKey(Configuration configuration, String keyName) {
        KeyProvider provider = getKeyProvider(configuration);
        try {
            provider.rollNewVersion(keyName);
            provider.flush();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not roll key %s with provider %s", keyName, provider), e);
        }
    }

    public static boolean keyExists(Configuration configuration, String keyName) {
        KeyProvider provider = getKeyProvider(configuration);
        try {
            List<String> keys = provider.getKeys();
            if (keys != null) {
                return keys.contains(keyName);
            }
            return false;
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Could not check if key %s exists using provider %s", keyName, provider), e);
        }
    }

    private static KeyProvider getKeyProvider(Configuration configuration) {
        KeyProvider provider = null;
        List<KeyProvider> providers;
        try {
            providers = KeyProviderFactory.getProviders(configuration);
            for (KeyProvider p : providers) {
                if (!p.isTransient()) {
                    provider = p;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (provider == null) {
            throw new RuntimeException(
                    "No KeyProvider has been specified.  Please check your core-site.xml (hadoop.security.key.provider.path) and hdfs-site.xml (dfs.encryption.key.provider.uri)");
        }

        return provider;
    }

    public static final long copyInputStreamToHdfsWithoutBomAndReturnRows(Configuration configuration,
            InputStream inputStream, String hdfsPath, long totalRows) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            try (OutputStream outputStream = fs.create(new Path(hdfsPath))) {
                return copyLarge(
                        new BOMInputStream(inputStream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                                ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                        outputStream, totalRows);
            }
        }
    }

    private static long copyLarge(InputStream input, OutputStream output, long totalRows) throws IOException {
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        boolean stop = false;
        int n = 0;
        long rows = 0;
        while (EOF != (n = input.read(buffer))) {
            if (!stop) {
                output.write(buffer, 0, n);
            }
            for (int i = 0; i < n; i++) {
                if (buffer[i] == '\n') {
                    rows++;
                    if (!stop && rows > totalRows) {
                        stop = true;
                    }
                }
            }
        }
        return rows;
    }
}

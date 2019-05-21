package com.latticeengines.common.exposed.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.StreamUtils;

import com.latticeengines.common.exposed.csv.LECSVFormat;

public class HdfsUtils {

    private static final Logger log = LoggerFactory.getLogger(HdfsUtils.class);
    private static final int EOF = -1;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

    public interface HdfsFileFormat {
        String AVRO_FILE = ".*.avro";
        String AVSC_FILE = ".*.avsc";
        String JSON_FILE = ".*.json";
    }

    public interface HdfsFilenameFilter {
        boolean accept(String filename);
    }

    public interface HdfsFileFilter {
        boolean accept(FileStatus file);
    }

    public enum LogFileEncodingType {
        NONE, GZ;

        public static LogFileEncodingType getEnum(String s) {
            if (NONE.name().equalsIgnoreCase(s)) {
                return NONE;
            } else if (GZ.name().equalsIgnoreCase(s)) {
                return GZ;
            }
            throw new IllegalArgumentException("No Enum specified for this string");
        }
    }

    public static FileSystem getFileSystem(Configuration configuration) throws IOException {
        return FileSystem.newInstance(configuration);
    }

    public static FileSystem getFileSystem(Configuration configuration, String path) throws IOException {
        if (path.startsWith("/")) {
            return FileSystem.newInstance(configuration);
        } else {
            return FileSystem.newInstance(URI.create(path), configuration);
        }
    }

    public static void mkdir(Configuration configuration, String dir) throws IOException {
        try (FileSystem fs = getFileSystem(configuration, dir)) {
            fs.mkdirs(new Path(dir));
        }
    }

    public static boolean isDirectory(Configuration configuration, String path) throws IOException {
        try (FileSystem fs = getFileSystem(configuration, path)) {
            return fs.isDirectory(new Path(path));
        }
    }

    public static void rmdir(Configuration configuration, String dir) throws IOException {
        try (FileSystem fs = getFileSystem(configuration, dir)) {
            fs.delete(new Path(dir), true);
        }
    }

    public static String getHdfsFileContents(Configuration configuration, String hdfsPath) throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsPath)) {
            Path schemaPath = new Path(hdfsPath);
            try (InputStream is = fs.open(schemaPath)) {
                try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                    StreamUtils.copy(is, os);
                    return new String(os.toByteArray());
                }
            }
        }
    }

    public static void copyInputStreamToHdfs(Configuration configuration, InputStream inputStream, String hdfsPath)
            throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsPath)) {
            try (OutputStream outputStream = fs.create(new Path(hdfsPath))) {
                IOUtils.copy(inputStream, outputStream);
            }
        }
    }

    public static void copyInputStreamToDest(URI scheme, Configuration configuration, InputStream inputStream)
            throws IOException {
        try (FileSystem fs = FileSystem.newInstance(scheme, configuration)) {
            try (OutputStream outputStream = fs.create(new Path(scheme.getPath()))) {
                IOUtils.copy(inputStream, outputStream);
            }
        }
    }

    public static void copyInputStreamToHdfsWithoutBom(Configuration configuration, InputStream inputStream,
            String hdfsPath) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            try (OutputStream outputStream = fs.create(new Path(hdfsPath))) {
                IOUtils.copy(new BOMInputStream(inputStream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                        ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE), outputStream);
            }
        }
    }

    public static void distcp(Configuration configuration, String srcPath, String tgtPath, String queue)
            throws Exception {
        DistCpOptions options = new DistCpOptions(new Path(srcPath), new Path(tgtPath));
        log.info("Running distcp from " + srcPath + " to " + tgtPath + " using queue " + queue);
        String[] args = new String[] { "-Dmapreduce.job.queuename=" + queue, "-update", srcPath, tgtPath };
        int exit = ToolRunner.run(new DistCp(configuration, options), args);
        if (exit != 0) {
            throw new RuntimeException("DistCp exited with code " + exit);
        }
        log.info("Finished distcp from " + srcPath + " to " + tgtPath + ".");
    }

    public static void copyLocalResourceToHdfs(Configuration configuration, String resourcePath, String hdfsPath)
            throws IOException {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource resource = resolver.getResource(resourcePath);
        copyLocalToHdfs(configuration, resource.getFile().getAbsolutePath(), hdfsPath);
    }

    public static void copyLocalToHdfs(Configuration configuration, String localPath, String hdfsPath)
            throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            fs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
        }
    }

    public static void copyFromLocalDirToHdfs(Configuration configuration, String localPath, String hdfsPath)
            throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsPath)) {
            FileUtil.copy(new File(localPath), fs, new Path(hdfsPath), false, configuration);
        }
    }

    public static void copyHdfsToLocal(Configuration configuration, String hdfsPath, String localPath)
            throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsPath)) {
            fs.copyToLocalFile(new Path(hdfsPath), new Path(localPath));
        }
    }

    public static void writeToFile(Configuration configuration, String hdfsPath, String contents) throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsPath)) {
            Path filePath = new Path(hdfsPath);

            try (BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(filePath, true)))) {
                br.write(contents);
            }
        }
    }

    public static void uncompressGZFileWithinHDFS(Configuration configuration, String gzHdfsPath,
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

    public static void uncompressZipFileWithinHDFS(Configuration configuration, String compressedFile,
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

    public static void compressGZFileWithinHDFS(Configuration configuration, String gzHdfsPath,
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

    public static boolean fileExists(Configuration configuration, String hdfsPath) throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsPath)) {
            return fs.exists(new Path(hdfsPath));
        }
    }

    public static List<String> getFilesForDir(Configuration configuration, String hdfsDir) throws IOException {
        return getFilesForDir(configuration, hdfsDir, (HdfsFilenameFilter) null);
    }

    public static List<String> getFilesForDir(Configuration configuration, String hdfsDir, final String regex)
            throws IOException {

        return getFilesForDir(configuration, hdfsDir, (HdfsFilenameFilter) filename -> {
            Pattern p = Pattern.compile(regex);
            Matcher matcher = p.matcher(filename);
            return matcher.matches();
        });
    }

    public static List<String> getFilesForDir(Configuration configuration, String hdfsDir, HdfsFilenameFilter filter)
            throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsDir)) {
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

    public static List<String> getFilesForDir(Configuration configuration, String hdfsDir, HdfsFileFilter filter)
            throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsDir)) {
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
    public static List<String> onlyGetFilesForDir(Configuration configuration, String hdfsDir, HdfsFileFilter filter)
            throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsDir)) {
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

    public static FileStatus getFileStatus(Configuration configuration, String hdfsDir) throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsDir)) {
            return fs.getFileStatus(new Path(hdfsDir));
        }
    }

    public static List<FileStatus> getFileStatusesForDir(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter) throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsDir)) {
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

    public static List<String> getFilesForDirRecursive(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter) throws IOException {
        return getFilesForDirRecursive(configuration, hdfsDir, filter, false);
    }

    public static List<String> getFilesForDirRecursive(Configuration configuration, String hdfsDir, String regex,
            boolean returnFirstMatch) throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsDir)) {
            FileStatus[] statuses = fs.listStatus(new Path(hdfsDir));
            Set<String> filePaths = new HashSet<String>();
            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    filePaths.addAll(getFilesForDir(configuration, status.getPath().toString(), regex));
                    if (returnFirstMatch && filePaths.size() > 0) {
                        break;
                    }
                    filePaths.addAll(getFilesForDirRecursive(configuration, status.getPath().toString(), regex,
                            returnFirstMatch));
                }
            }
            return new ArrayList<>(filePaths);
        }
    }

    public static List<String> getFilesForDirRecursive(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter, boolean returnFirstMatch) throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsDir)) {
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

    public static List<String> getFilesForDirRecursiveWithFilterOnDir(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter, HdfsFileFilter folderFilter) throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsDir)) {
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
    public static List<String> onlyGetFilesForDirRecursive(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter, boolean returnFirstMatch) throws IOException {
        Set<String> filePaths = new HashSet<String>();
        onlyGetFilesForDirRecursiveHelper(configuration, hdfsDir, filter, returnFirstMatch, filePaths);
        return new ArrayList<>(filePaths);
    }

    public static void onlyGetFilesForDirRecursiveHelper(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter, boolean returnFirstMatch, Set<String> filePaths) throws IOException {
        if (returnFirstMatch && filePaths.size() > 0) {
            return;
        }
        try (FileSystem fs = getFileSystem(configuration, hdfsDir)) {
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

    public static List<FileStatus> getFileStatusesForDirRecursive(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter) throws IOException {
        return getFileStatusesForDirRecursive(configuration, hdfsDir, filter, false);
    }

    public static List<FileStatus> getFileStatusesForDirRecursive(Configuration configuration, String hdfsDir,
            HdfsFileFilter filter, boolean returnFirstMatch) throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsDir)) {
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

    public static String getApplicationLog(Configuration configuration, String user, String applicationId)
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
        FileSystem fs = getFileSystem(configuration, hdfsPath);
        return fs.open(new Path(hdfsPath));
    }

    public static void copyFromLocalToHdfs(Configuration configuration, String localPath, String hdfsPath)
            throws IOException {
        try (FileSystem fs = getFileSystem(configuration, hdfsPath)) {
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
        return getFilesByGlobWithScheme(configuration, globPath, false);
    }

    public static List<String> getFilesByGlobWithScheme(Configuration configuration, String globPath,
            boolean withScheme) throws IOException {
        try (FileSystem fs = getFileSystem(configuration, globPath)) {
            FileStatus[] statuses = fs.globStatus(new Path(globPath));
            List<String> filePaths = new ArrayList<>();
            if (statuses == null) {
                return filePaths;
            }
            for (FileStatus status : statuses) {
                Path filePath = status.getPath();
                if (!withScheme) {
                    filePaths.add(Path.getPathWithoutSchemeAndAuthority(filePath).toString());
                } else {
                    filePaths.add(filePath.toString());
                }
            }
            return filePaths;
        }
    }

    public static boolean moveFile(Configuration configuration, String src, String dst) throws IOException {
        if (inDifferentEncryptionZone(configuration, src, dst)) {
            log.info("Using copy instead of move.");
            if (copyFiles(configuration, src, dst)) {
                rmdir(configuration, src);
                return true;
            } else {
                return false;
            }
        }
        try (FileSystem fs = getFileSystem(configuration, src)) {
            return fs.rename(new Path(src), new Path(dst));
        }
    }

    public static boolean inDifferentEncryptionZone(Configuration configuration, String src, String dst)
            throws IOException {
        return false;
    }

    public static boolean rename(Configuration configuration, String src, String dst) throws IOException {
        try (FileSystem fs = getFileSystem(configuration, src)) {
            return fs.rename(new Path(src), new Path(dst));
        }
    }

    public static void moveGlobToDir(Configuration configuration, String sourceGlob, String targetDir)
            throws IOException {
        if (!isDirectory(configuration, targetDir)) {
            mkdir(configuration, targetDir);
        }
        for (String filePath : getFilesByGlob(configuration, sourceGlob)) {
            String fileName = new Path(filePath).getName();
            moveFile(configuration, filePath, new Path(targetDir, fileName).toString());
        }
    }

    private static boolean keyEquals(EncryptionZone zone1, EncryptionZone zone2) {
        return zone1.getKeyName().equals(zone2.getKeyName()) && zone1.getVersion().equals(zone2.getVersion());
    }

    public static boolean copyFiles(Configuration configuration, String src, String dst)
            throws IllegalArgumentException, IOException {
        try (FileSystem srcFS = getFileSystem(configuration, src);
                FileSystem dstFS = getFileSystem(configuration, dst)) {
            return FileUtil.copy(srcFS, new Path(src), dstFS, new Path(dst), false, false, configuration);
        }
    }

    public static void copyGlobToDirWithScheme(Configuration configuration, String sourceGlob, String targetDir,
            String tgtNameSuffix) throws IOException {
        if (!isDirectory(configuration, targetDir)) {
            mkdir(configuration, targetDir);
        }
        for (String filePath : getFilesByGlobWithScheme(configuration, sourceGlob, true)) {
            String fileName = new Path(filePath).getName();
            fileName = appendSuffixToFileName(fileName, tgtNameSuffix);
            copyFiles(configuration, filePath, new Path(targetDir, fileName).toString());
        }
    }

    public static void copyGlobToDir(Configuration configuration, String sourceGlob, String targetDir,
            String tgtNameSuffix) throws IOException {
        if (!isDirectory(configuration, targetDir)) {
            mkdir(configuration, targetDir);
        }
        for (String filePath : getFilesByGlob(configuration, sourceGlob)) {
            String fileName = new Path(filePath).getName();
            fileName = appendSuffixToFileName(fileName, tgtNameSuffix);
            copyFiles(configuration, filePath, new Path(targetDir, fileName).toString());
        }
    }

    public static String appendSuffixToFileName(String fileName, String suffix) {
        if (StringUtils.isBlank(suffix)) {
            return fileName;
        }
        if (StringUtils.isEmpty(fileName)) {
            return suffix;
        }
        if (!fileName.contains(".")) {
            return fileName + suffix;
        }
        int dotIndex = fileName.indexOf(".");
        return fileName.substring(0, dotIndex) + suffix + fileName.substring(dotIndex);
    }

    public static FileChecksum getCheckSum(Configuration configuration, String path) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            return fs.getFileChecksum(new Path(path));
        }
    }

    public static Long getFileSize(Configuration configuration, String filePath) throws IOException {
        try (FileSystem fs = getFileSystem(configuration, filePath)) {
            FileStatus status = fs.getFileStatus(new Path(filePath));
            return status.getLen();
        }
    }

    public static Long count(Configuration configuration, String filePath) throws IOException {
        long count = 0;
        try (FileSystem fs = getFileSystem(configuration, filePath)) {
            Path path = new Path(filePath);
            try (InputStream is = fs.open(path)) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                    while (reader.readLine() != null) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    public static long copyInputStreamToHdfsWithoutBomAndReturnRows(Configuration configuration,
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

    public static long copyCSVStreamToHdfs(Configuration configuration, InputStream inputStream, String hdfsPath,
                                             long totalRows) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            try (OutputStream outputStream = fs.create(new Path(hdfsPath))) {
                CSVFormat format = LECSVFormat.format.withFirstRecordAsHeader();
                try (CSVParser parser = new CSVParser(new BufferedReader(new InputStreamReader(
                        new BOMInputStream(inputStream, false, ByteOrderMark.UTF_8,
                                ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE,
                                ByteOrderMark.UTF_32BE), StandardCharsets.UTF_8)), format)) {
                    try (CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8),
                            LECSVFormat.format.withHeader(parser.getHeaderMap().keySet().toArray(new String[] {})))) {
                        Iterator<CSVRecord> iterator = parser.iterator();
                        long rows = 0;
                        while (iterator.hasNext()) {
                            CSVRecord item = iterator.next();
                            csvPrinter.printRecord(item);
                            rows++;
                            if (item.getRecordNumber() > totalRows) {
                                break;
                            }
                        }
                        return rows;
                    }
                }
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

    public static long copyInputStreamToHdfsWithoutBomAndReturnRows(Configuration configuration,
                                                                    InputStream inputStream, String hdfsPath) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            try (OutputStream outputStream = fs.create(new Path(hdfsPath))) {
                return copyLarge(
                        new BOMInputStream(inputStream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                                ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                        outputStream);
            }
        }
    }

    private static long copyLarge(InputStream input, OutputStream output) throws IOException {
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        int n = 0;
        long rows = 0;
        while (EOF != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            for (int i = 0; i < n; i++) {
                if (buffer[i] == '\n') {
                    rows++;
                }
            }
        }
        return rows;
    }

}

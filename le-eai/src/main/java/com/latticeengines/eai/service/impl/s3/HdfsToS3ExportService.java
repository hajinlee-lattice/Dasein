package com.latticeengines.eai.service.impl.s3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.HdfsToS3Configuration;

/**
 * This one was suppose to be a new type of camel route service, but the camel
 * s3 component is bad. So we falls back to native aws s3 sdk. To avoid creating
 * a new ItemProcessor, this one is still invoked in CamelRouteProcessor
 */
@Component("hdfsToS3ExportService")
public class HdfsToS3ExportService {

    private static final Log log = LogFactory.getLog(HdfsToS3ExportService.class);

    private static final Long MIN_SPLIT_SIZE = 100L * 1024L * 1024L; // 100 GB
    private static final String LOCAL_CACHE = "tmp/camel";

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private S3Service s3Service;

    public void upload(HdfsToS3Configuration config) {
        String bucket = config.getS3Bucket();
        String prefix = config.getS3Prefix();
        s3Service.uploadLocalDirectory(bucket, prefix, LOCAL_CACHE, true);
    }

    private Boolean shouldSplit(HdfsToS3Configuration config) {
        if (config.getSplitSize() == null) {
            return false;
        } else if (config.getSplitSize() >= MIN_SPLIT_SIZE) {
            return true;
        } else {
            throw new IllegalArgumentException("Split size too small, must be at least " + MIN_SPLIT_SIZE);
        }
    }

    private String splitFileName(HdfsToS3Configuration config, Integer splitIdx) {
        String originalName = config.getTargetFilename();
        String withoutExt = originalName.substring(0, originalName.indexOf("."));
        String newWithOutExt = String.format("%s-%05d", withoutExt, splitIdx);
        return originalName.replace(withoutExt, newWithOutExt);
    }

    public void downloadToLocal(HdfsToS3Configuration config) {
        Long splitSize = config.getSplitSize();
        String hdfsPath = config.getExportInputPath();
        String fileName = config.getTargetFilename();

        if (!fileName.endsWith(".avro")) {
            throw new IllegalArgumentException("Splitting only works for avro file now.");
        }

        Double downloadProgress = 0.0;
        if (shouldSplit(config)) {
            log.info("Downloading original file into splits on local.");
            Long totalRecords = AvroUtils.count(yarnConfiguration, hdfsPath);
            log.info("Found " + totalRecords + " records in input file.");

            Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, hdfsPath);
            Iterator<GenericRecord> iterator = AvroUtils.iterator(yarnConfiguration, hdfsPath);

            Integer splitIdx = 0;
            while (iterator.hasNext()) {
                String splitFileName = splitFileName(config, splitIdx);
                File avroFile = new File("tmp/camel/" + splitFileName);
                try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(
                        new GenericDatumWriter<GenericRecord>())) {
                    FileUtils.touch(avroFile);
                    writer.create(schema, avroFile);
                    Long fileSize = FileUtils.sizeOf(avroFile);
                    Long recordsInFile = 0L;
                    while (fileSize < splitSize && iterator.hasNext()) {
                        GenericRecord datum = iterator.next();
                        writer.append(datum);
                        recordsInFile++;
                        fileSize = FileUtils.sizeOf(avroFile);
                    }
                    downloadProgress += recordsInFile.doubleValue() / totalRecords.doubleValue();
                    log.info("Downloaded " + recordsInFile + " records to split " + splitFileName + String.format(
                            " (%.2f MB): %.2f %%", fileSize.doubleValue() / 1024.0 / 1024.0, downloadProgress * 100));
                } catch (IOException e) {
                    throw new RuntimeException("Failed to write to split file " + splitFileName, e);
                }
                splitIdx++;
            }
        } else {
            log.info("split_size is not specified. download the whole file.");
            try {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, hdfsPath, LOCAL_CACHE + "/" + fileName);
            } catch (Exception e) {
                log.error("Failed to download the file " + hdfsPath + " from hdfs to local", e);
            }
        }
        log.info("Downloading finished.");
    }

    public void parallelDownloadToLocal(HdfsToS3Configuration config) {
        Long splitSize = config.getSplitSize();
        String hdfsPath = config.getExportInputPath();
        String fileName = config.getTargetFilename();

        if (!fileName.endsWith(".avro")) {
            throw new IllegalArgumentException("Splitting only works for avro file now.");
        }

        Long totalRecords = AvroUtils.count(yarnConfiguration, hdfsPath);
        log.info("Found " + totalRecords + " records in input files.");

        List<String> filePaths = new ArrayList<>();
        try {
            filePaths = HdfsUtils.getFilesByGlob(yarnConfiguration, hdfsPath);
        } catch (IOException e) {
            log.error("Failed to find files from " + hdfsPath, e);
        }
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, hdfsPath);
        Double downloadProgress = 0.0;
        Long count = 0L;
        ExecutorService executorService = Executors.newFixedThreadPool(Math.min(8, filePaths.size()));
        Map<String, Future<Long>> futures = new LinkedHashMap<>();
        for (String filePath : filePaths) {
            futures.put(filePath, executorService.submit(new Callable<Long>() {

                Integer splitIdx = 0;

                @Override
                public Long call() throws Exception {
                    return copyToLocal(filePath);
                }

                protected Long copyToLocal(String filePath) throws IllegalArgumentException, IOException {
                    if (config.getSplitSize() == null
                            || config.getSplitSize() >= HdfsUtils.getFileSize(yarnConfiguration, filePath)) {
                        log.info("split_size is either not specified or too larget. download the whole file.");
                        File localFile = new File(LOCAL_CACHE + "/" + new Path(filePath).getName());
                        try {
                            HdfsUtils.copyHdfsToLocal(yarnConfiguration, filePath, localFile.getPath());
                            FileUtils.deleteQuietly(
                                    new File(LOCAL_CACHE + "/." + new Path(filePath).getName() + ".crc"));
                            Configuration localConfig = new Configuration();
                            localConfig.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
                            long count = AvroUtils.count(localConfig, localFile.getPath());
                            log.info("Downloaded " + count + " records"
                                    + String.format(" (%.2f MB)", FileUtils.sizeOf(localFile) / 1024.0 / 1024.0));
                            return count;
                        } catch (Exception e) {
                            log.error("Failed to download the file " + filePath + " from hdfs to local", e);
                        }
                    } else {
                        log.info("Downloading original file " + filePath + " into splits on local.");
                        Iterator<GenericRecord> iterator = AvroUtils.iterator(yarnConfiguration, filePath);
                        Long recordsInFile = 0L;
                        while (iterator.hasNext()) {
                            String splitFileName = new Path(filePath).getName().replace(".avro",
                                    "-" + splitIdx + ".avro");
                            File avroFile = new File(LOCAL_CACHE + "/" + splitFileName);
                            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(
                                    new GenericDatumWriter<GenericRecord>())) {
                                FileUtils.touch(avroFile);
                                writer.create(schema, avroFile);
                                Long fileSize = FileUtils.sizeOf(avroFile);
                                while (fileSize < splitSize && iterator.hasNext()) {
                                    GenericRecord datum = iterator.next();
                                    writer.append(datum);
                                    recordsInFile++;
                                    fileSize = FileUtils.sizeOf(avroFile);
                                }
                                log.info("Downloaded " + recordsInFile + " records to split " + splitFileName
                                        + String.format(" (%.2f MB)", fileSize.doubleValue() / 1024.0 / 1024.0));
                            } catch (IOException e) {
                                throw new RuntimeException("Failed to write to split file " + splitFileName, e);
                            }
                            splitIdx++;
                        }
                        return recordsInFile;
                    }
                    return 0L;
                }
            }));

            for (Map.Entry<String, Future<Long>> entry : futures.entrySet()) {
                String file = entry.getKey();
                try {
                    count += entry.getValue().get();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to count file " + file, e);
                }
                downloadProgress = count.doubleValue() / totalRecords.doubleValue();
                log.info(String.format("Current Progress: %.2f %%", downloadProgress * 100));
            }

        }

        log.info("Downloading finished.");
    }

}

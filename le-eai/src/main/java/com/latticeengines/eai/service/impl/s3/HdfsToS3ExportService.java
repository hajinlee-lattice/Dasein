package com.latticeengines.eai.service.impl.s3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.HdfsToS3Configuration;
import com.latticeengines.eai.runtime.service.EaiRuntimeService;

/**
 * This one was suppose to be a new type of camel route service, but the camel
 * s3 component is bad. So we falls back to native aws s3 sdk. To avoid creating
 * a new ItemProcessor, this one is still invoked in CamelRouteProcessor
 */
@Component("hdfsToS3ExportService")
public class HdfsToS3ExportService extends EaiRuntimeService<HdfsToS3Configuration> {

    private static final Logger log = LoggerFactory.getLogger(HdfsToS3ExportService.class);

    private static final Long MIN_SPLIT_SIZE = 10L * 1024L * 1024L; // 10 MB
    private static final String LOCAL_CACHE = "tmp";

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private S3Service s3Service;

    @Override
    public void invoke(HdfsToS3Configuration configuration) {
        int numFiles = downloadToLocal(configuration);
        setProgress(0.30f);
        if (numFiles > 0) {
            upload(configuration);
        }
        setProgress(0.99f);
    }

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

    public int downloadToLocal(HdfsToS3Configuration config) {
        Long splitSize = config.getSplitSize();
        String hdfsPath = config.getExportInputPath();
        String fileName = config.getTargetFilename();

        if (!fileName.endsWith(".avro")) {
            throw new IllegalArgumentException("Splitting only works for avro file now.");
        }

        double downloadProgress = 0.0;
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
                    long fileSize = FileUtils.sizeOf(avroFile);
                    long recordsInFile = 0L;
                    while (fileSize < splitSize && iterator.hasNext()) {
                        GenericRecord datum = iterator.next();
                        writer.append(datum);
                        recordsInFile++;
                        fileSize = FileUtils.sizeOf(avroFile);
                    }
                    downloadProgress += (double) recordsInFile / totalRecords.doubleValue();
                    log.info("Downloaded " + recordsInFile + " records to split " + splitFileName + String.format(
                            " (%.2f MB): %.2f %%", (double) fileSize / 1024.0 / 1024.0, downloadProgress * 100));
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
        int numAvroFiles = CollectionUtils.size( //
                FileUtils.listFiles(new File(LOCAL_CACHE), new String[]{ "avro" }, false));
        log.info("Downloading " + numAvroFiles + " non-empty avro files finished.");
        return numAvroFiles;
    }

    @SuppressWarnings("checkstyle:AnonInnerLength")
    public int parallelDownloadToLocal(HdfsToS3Configuration config) {
        final boolean needToSplit = shouldSplit(config);

        String hdfsPath = config.getExportInputPath();
        String fileName = config.getTargetFilename();

        if (!fileName.endsWith(".avro")) {
            throw new IllegalArgumentException("Splitting only works for avro file now.");
        }

        Long totalRecords = 0L;
        if (needToSplit) {
            totalRecords = AvroUtils.count(yarnConfiguration, hdfsPath);
            log.info("Found " + totalRecords + " records in input files.");
        }

        List<String> filePaths = new ArrayList<>();
        try {
            filePaths = HdfsUtils.getFilesByGlob(yarnConfiguration, hdfsPath);
        } catch (IOException e) {
            log.error("Failed to find files from " + hdfsPath, e);
        }
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, hdfsPath);
        Long count = 0L;
        ExecutorService executorService = Executors.newFixedThreadPool(Math.min(8, filePaths.size()));
        Map<String, Future<Long>> futures = new LinkedHashMap<>();
        for (final String filePath : filePaths) {
            long splitSize = needToSplit ? config.getSplitSize() : -1L;
            futures.put(filePath, executorService.submit( //
                    new HdfsToS3Uploader(yarnConfiguration, LOCAL_CACHE, needToSplit, splitSize, schema, filePath) //
            ));
        }
        for (Map.Entry<String, Future<Long>> entry : futures.entrySet()) {
            String file = entry.getKey();
            try {
                count += entry.getValue().get();
            } catch (Exception e) {
                throw new RuntimeException("Failed to count file " + file, e);
            }
            if (needToSplit) {
                double downloadProgress = count.doubleValue() / totalRecords.doubleValue();
                log.info(String.format("Current Progress: %.2f %%", downloadProgress * 100));
            } else {
                double downloadProgress = count.doubleValue() / filePaths.size();
                log.info(String.format("Current Progress: %.2f %%", downloadProgress * 100));
            }
        }
        executorService.shutdown();

        int numAvroFiles = CollectionUtils.size( //
                FileUtils.listFiles(new File(LOCAL_CACHE), new String[]{ "avro" }, false));
        log.info("Downloading " + numAvroFiles + " non-empty avro files finished.");
        return numAvroFiles;
    }

}

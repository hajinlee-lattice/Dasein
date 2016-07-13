package com.latticeengines.eai.service.impl.camel;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.AmazonS3;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.route.HdfsToS3RouteConfiguration;

/**
 * This one was suppose to be a new type of camel route service, but the camel
 * s3 component is bad. So we falls back to native aws s3 sdk. To avoid creating
 * a new ItemProcessor, this one is still invoked in CamelRouteProcessor
 */
@Component("hdfsToS3RouteService")
public class HdfsToS3RouteService {

    private static final Log log = LogFactory.getLog(HdfsToS3RouteService.class);

    private static final Long MIN_SPLIT_SIZE = 10L * 1024L * 1024L;
    private static final String LOCAL_CACHE = "tmp/camel";

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private AmazonS3 s3Client;

    @Autowired
    private S3Service s3Service;

    private Integer numSplits = 1;

    public void upload(HdfsToS3RouteConfiguration config) {
        String bucket = config.getS3Bucket();
        String prefix = config.getS3Prefix();
        s3Service.uploadLocalDirectory(bucket, prefix, LOCAL_CACHE, true);
    }

    private Boolean shouldSplit(HdfsToS3RouteConfiguration config) {
        if (config.getSplitSize() == null) {
            return false;
        } else if (config.getSplitSize() >= MIN_SPLIT_SIZE) {
            return true;
        } else {
            throw new IllegalArgumentException("Split size too small, must be at least " + MIN_SPLIT_SIZE);
        }
    }

    private String splitFileName(HdfsToS3RouteConfiguration config, Integer splitIdx) {
        String originalName = config.getTargetFilename();
        String withoutExt = originalName.substring(0, originalName.indexOf("."));
        String newWithOutExt = String.format("%s-%05d", withoutExt, splitIdx);
        return originalName.replace(withoutExt, newWithOutExt);
    }

    public void downloadToLocal(HdfsToS3RouteConfiguration config) {
        Long splitSize = config.getSplitSize();
        String hdfsPath = config.getHdfsPath();
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
                    log.info("Downloaded " + recordsInFile + " records to split " + splitFileName + String
                            .format(" (%.2f MB): %.2f %%", fileSize.doubleValue() / 1024.0 / 1024.0, downloadProgress * 100));
                } catch (IOException e) {
                    throw new RuntimeException("Failed to write to split file " + splitFileName, e);
                }
                splitIdx++;
            }
            numSplits = splitIdx;
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

}

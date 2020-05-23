package com.latticeengines.eai.service.impl.s3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;

class HdfsToS3Uploader implements Callable<Long> {

    private static final Logger log = LoggerFactory.getLogger(HdfsToS3Uploader.class);

    Integer splitIdx = 0;

    private final Configuration yarnConfiguration;
    private final boolean needToSplit;
    private final String localCache;
    private final Schema schema;
    private final String filePath;
    private final long splitSize;

    HdfsToS3Uploader(Configuration yarnConfiguration, String localCache, boolean needToSplit, long splitSize, //
                     Schema schema, String filePath) {
        this.yarnConfiguration = yarnConfiguration;
        this.localCache = localCache;
        this.needToSplit = needToSplit;
        this.splitSize = splitSize;
        this.schema = schema;
        this.filePath = filePath;
    }

    @Override
    public Long call() {
        if (needToSplit) {
            return splitToLocal(filePath);
        } else {
            return downloadToLocal(filePath);
        }
    }

    private Long downloadToLocal(String filePath) {
        log.info("Downloading original file " + filePath + " to local as a whole.");
        String fileName = new Path(filePath).getName();
        try {
            boolean hasRecords = AvroUtils.hasRecords(yarnConfiguration, filePath);
            if (hasRecords) {
                File avroFile = new File(localCache + "/" + fileName);
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, filePath, avroFile.getAbsolutePath());
                FileUtils.deleteQuietly(new File(localCache + "/." + fileName + ".crc"));
                long fileSize = FileUtils.sizeOf(avroFile);
                log.info("Downloaded filePath to " + fileName
                        + String.format(" (%.2f MB)", (double) fileSize / 1024.0 / 1024.0));
            } else {
                log.warn("Avro file " + filePath + " has 0 rows, skip download to local.");
            }
            return 1L;
        } catch (IOException e) {
            throw new RuntimeException("Failed to download to split file " + fileName, e);
        }
    }

    private Long splitToLocal(String filePath) throws IllegalArgumentException {
        log.info("Downloading original file " + filePath + " to local, and split into chunks of "
                + splitSize / 1024.0 / 1024.0 + " MB.");
        Iterator<GenericRecord> iterator = AvroUtils.iterateAvroFiles(yarnConfiguration, filePath);
        Long recordsInFile = 0L;
        while (iterator.hasNext()) {
            String fileName = new Path(filePath).getName().replace(".avro", "-" + splitIdx + ".avro");
            File avroFile = new File(localCache + "/" + fileName);
            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(
                    new GenericDatumWriter<>())) {
                writer.setCodec(CodecFactory.snappyCodec());
                FileUtils.touch(avroFile);
                long fileSize = FileUtils.sizeOf(avroFile);
                if (fileSize == 0) {
                    writer.create(schema, avroFile);
                }
                long fileSizeIncrement = 0L;
                while (fileSizeIncrement < splitSize && iterator.hasNext()) {
                    GenericRecord datum = iterator.next();
                    writer.append(datum);
                    recordsInFile++;
                    fileSizeIncrement = FileUtils.sizeOf(avroFile) - fileSize;
                }
                fileSize = FileUtils.sizeOf(avroFile);
                log.info("Downloaded " + recordsInFile + " records to " + fileName
                        + String.format(" (%.2f MB)", (double) fileSize / 1024.0 / 1024.0));
            } catch (IOException e) {
                throw new RuntimeException("Failed to write to split file " + fileName, e);
            }
            splitIdx++;
        }
        return recordsInFile;
    }

    @SuppressWarnings("unused")
    private Long copyToLocalJson(String filePath) throws IllegalArgumentException, IOException {
        log.info("Downloading original file " + filePath + " to local.");
        Iterator<GenericRecord> iterator = AvroUtils.iterateAvroFiles(yarnConfiguration, filePath);
        Long recordsInFile = 0L;
        String fileName = new Path(filePath).getName().replace(".avro", ".json");
        File jsonFile = new File(localCache + "/" + fileName);
        FileUtils.touch(jsonFile);
        try (BufferedWriter writer = new BufferedWriter(
                new FileWriter(localCache + "/" + fileName, true))) {
            while (iterator.hasNext()) {
                GenericRecord datum = iterator.next();
                writer.write(datum.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to split file " + fileName, e);
        }
        long fileSize = FileUtils.sizeOf(jsonFile);
        log.info("Downloaded " + recordsInFile + " records to " + fileName
                + String.format(" (%.2f MB)", (double) fileSize / 1024.0 / 1024.0));
        return recordsInFile;
    }
}

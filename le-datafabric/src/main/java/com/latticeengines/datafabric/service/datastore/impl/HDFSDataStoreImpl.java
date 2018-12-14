package com.latticeengines.datafabric.service.datastore.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;

public class HDFSDataStoreImpl implements FabricDataStore {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(HDFSDataStoreImpl.class);

    public static final String DATE_FORMAT_STRING = "yyyy-MM-dd_z";
    public static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
    public static final String UTC = "UTC";

    private static final TimeZone TIME_ZONE = TimeZone.getTimeZone(UTC);

    private Configuration config;
    private String fileName;
    @SuppressWarnings("unused")
    private String recordType;
    private Schema schema;
    private String repositoryDir;
    private String baseDir;
    private String localDir;
    private boolean appendable;

    static {
        dateFormat.setTimeZone(TIME_ZONE);
    }

    public HDFSDataStoreImpl(Configuration config, String baseDir, String localDir, String repositoryDir,
            String fileName, String recordType, Schema schema, boolean appendable) {
        this.config = config;
        this.baseDir = baseDir;
        this.localDir = localDir;
        this.repositoryDir = repositoryDir;
        this.fileName = fileName;
        this.recordType = recordType;
        this.schema = schema;
        this.appendable = appendable;

    }

    @Override
    public void createRecord(String id, Pair<GenericRecord, Map<String, Object>> pair) {

        String fullPath = getFilePath();
        try {
            if (appendable) {
                if (!HdfsUtils.fileExists(config, fullPath)) {
                    AvroUtils.writeToHdfsFile(config, schema, fullPath, Arrays.asList(pair.getKey()));
                } else {
                    AvroUtils.appendToHdfsFile(config, fullPath, Arrays.asList(pair.getKey()));
                }
            } else {
                fullPath = FilenameUtils.removeExtension(fullPath) + UUID.randomUUID().toString() + "."
                        + FilenameUtils.getExtension(fullPath);
                AvroUtils.writeToHdfsFile(config, schema, fullPath, Arrays.asList(pair.getKey()));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private String getFilePath() {
        Calendar c = Calendar.getInstance(TIME_ZONE);
        c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        String dateStr = dateFormat.format(c.getTime());
        return baseDir + "/" + repositoryDir + "/Snapshot/" + dateStr + "/" + fileName;
    }

    @Override
    public void updateRecord(String id, Pair<GenericRecord, Map<String, Object>> pair) {
        throw new UnsupportedOperationException("updateRecord");
    }

    @Override
    public void createRecords(Map<String, Pair<GenericRecord, Map<String, Object>>> pairs) {
        String fullPath = getFilePath();
        List<GenericRecord> records = new ArrayList<>();
        for (Map.Entry<String, Pair<GenericRecord, Map<String, Object>>> pair : pairs.entrySet()) {
            records.add(pair.getValue().getKey());
        }
        try {
            if (appendable) {
                if (!HdfsUtils.fileExists(config, fullPath)) {
                    AvroUtils.writeToHdfsFile(config, schema, fullPath, records);
                } else {
                    AvroUtils.appendToHdfsFile(config, fullPath, records);
                }
            } else {
                if (StringUtils.isBlank(localDir)) {
                    writeWithoutAppend(fullPath, records);
                } else {
                    simulateAppend(fullPath, records);
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void simulateAppend(String fullPath, List<GenericRecord> records) throws IOException {
        String localFilePath = localDir + "/" + FilenameUtils.getName(fullPath);
        if (!HdfsUtils.fileExists(config, "file:" + localFilePath)) {
            AvroUtils.writeToLocalFile(schema, records, localFilePath);
        } else {
            AvroUtils.appendToLocalFile(records, localFilePath);
        }
        if (HdfsUtils.getFileSize(config, "file:" + localFilePath) > 100_000) {
            String newLocalFilePath = FilenameUtils.removeExtension(localFilePath) + "-" + System.currentTimeMillis()
                    + "." + FilenameUtils.getExtension(localFilePath);
            HdfsUtils.rename(config, localFilePath, newLocalFilePath);
            String tgtFilePath = FilenameUtils.getFullPath(fullPath) + FilenameUtils.getName(newLocalFilePath);
            HdfsUtils.copyFromLocalToHdfs(config, newLocalFilePath, tgtFilePath);
            HdfsUtils.rmdir(config, newLocalFilePath);
        }
    }

    private void writeWithoutAppend(String fullPath, List<GenericRecord> records) throws IOException {
        fullPath = FilenameUtils.removeExtension(fullPath) + "-" + System.currentTimeMillis() + "."
                + FilenameUtils.getExtension(fullPath);
        AvroUtils.writeToHdfsFile(config, schema, fullPath, records);
    }

    @Override
    public Pair<GenericRecord, Map<String, Object>> findRecord(String id) {
        throw new UnsupportedOperationException("findRecord");
    }

    @Override
    public Map<String, Pair<GenericRecord, Map<String, Object>>> batchFindRecord(List<String> idList) {
        throw new UnsupportedOperationException("batchFindRecord");
    }

    @Override
    public List<Pair<GenericRecord, Map<String, Object>>> findRecords(Map<String, String> properties) {
        throw new UnsupportedOperationException("findRecords");
    }

    @Override
    public void deleteRecord(String id, GenericRecord record) {
        throw new UnsupportedOperationException("deleteRecord");
    }

    @Override
    public Map<String, Object> findAttributes(String id) {
        throw new UnsupportedOperationException("findAttributes");
    }

}

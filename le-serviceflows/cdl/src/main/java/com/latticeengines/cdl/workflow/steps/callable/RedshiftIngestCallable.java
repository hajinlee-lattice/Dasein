package com.latticeengines.cdl.workflow.steps.callable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.query.DataPage;

public abstract class RedshiftIngestCallable<T> implements Callable<T> {

    private static final Logger log = LoggerFactory.getLogger(RedshiftIngestCallable.class);

    private int pageSize;
    private int rowsPerFile;

    protected abstract long getTotalCount();

    protected abstract DataPage fetchPage(long ingestedCount, long pageSize);

    protected abstract GenericRecord parseData(Map<String, Object> data);

    protected abstract Schema getAvroSchema();

    protected abstract void preIngest();

    protected abstract T postIngest();

    private final String hdfsPath;
    private long totalCount;
    private long ingestedCount = 0L;
    private final Configuration yarnConfiguration;

    public RedshiftIngestCallable(Configuration yarnConfiguration, String hdfsPath) {
        this.yarnConfiguration = yarnConfiguration;
        this.hdfsPath = hdfsPath;
    }

    @Override
    public T call() {
        preIngest();
        totalCount = getTotalCount();
        ingestPageByPage();
        return postIngest();
    }

    private void ingestPageByPage() {
        Schema schema = getAvroSchema();
        int recordsInCurrentFile = 0;
        int fileId = 0;
        String targetFile = prepareTargetFile(fileId);
        List<Map<String, Object>> data = new ArrayList<>();
        do {
            DataPage dataPage = fetchPage(ingestedCount, pageSize);
            if (dataPage != null) {
                data = dataPage.getData();
            }
            if (CollectionUtils.isNotEmpty(data)) {
                List<GenericRecord> records = dataPageToRecords(data);
                recordsInCurrentFile += records.size();
                try {
                    if (!HdfsUtils.fileExists(yarnConfiguration, targetFile)) {
                        log.info("Start dumping " + records.size() + " records to " + targetFile);
                        AvroUtils.writeToHdfsFile(yarnConfiguration, schema, targetFile, records, true);
                    } else {
                        log.info("Appending " + records.size() + " records to " + targetFile);
                        AvroUtils.appendToHdfsFile(yarnConfiguration, targetFile, records, true);
                        if (recordsInCurrentFile >= rowsPerFile) {
                            fileId++;
                            targetFile = prepareTargetFile(fileId);
                            recordsInCurrentFile = 0;
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to write to avro file " + targetFile, e);
                }
                ingestedCount += records.size();
                if (pageSize != records.size()) {
                    log.info("Change page size from " + pageSize + " to " + records.size());
                    pageSize = records.size();
                }
                log.info(String.format("Ingested %d / %d records with page size %d.", ingestedCount, totalCount,
                        pageSize));
            }
        } while (ingestedCount < totalCount && CollectionUtils.isNotEmpty(data));
    }

    private List<GenericRecord> dataPageToRecords(Iterable<Map<String, Object>> data) {
        List<GenericRecord> records = new ArrayList<>();
        data.forEach(map -> {
            GenericRecord record = parseData(map);
            records.add(record);
        });
        return records;
    }

    protected String prepareTargetFile(int fileId) {
        String targetFile = String.format("%s/part-%05d.avro", hdfsPath, fileId);
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetFile)) {
                HdfsUtils.rmdir(yarnConfiguration, targetFile);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to cleanup target file " + targetFile);
        }
        return targetFile;
    }

    protected String getHdfsPath() {
        return hdfsPath;
    }

    protected Configuration getYarnConfiguration() {
        return yarnConfiguration;
    }

    protected long getIngestedCount() {
        return ingestedCount;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public void setRowsPerFile(int rowsPerFile) {
        this.rowsPerFile = rowsPerFile;
    }

}

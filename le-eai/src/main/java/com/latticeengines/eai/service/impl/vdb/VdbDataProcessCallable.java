package com.latticeengines.eai.service.impl.vdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbQueryDataResult;
import com.latticeengines.domain.exposed.pls.VdbQueryResultColumn;
import com.latticeengines.eai.routes.DataContainer;
import com.latticeengines.eai.service.impl.vdb.converter.VdbValueConverter;

public class VdbDataProcessCallable implements Callable<Integer[]> {

    private static final Logger log = LoggerFactory.getLogger(VdbDataProcessCallable.class);
    private static final String ERROR_FILE = "error.csv";
    private static final String DUPLICATE_FILE = "duplicate.csv";

    private static final int MAX_RETRIES = 3;

    private String processorId;
    private boolean stop;
    private VdbValueConverter vdbValueConverter;
    private Table table;
    private Queue<String> fileQueue;
    private String extractPath;
    private String avroFileName;
    private boolean needDedup;
    private Set<String> uniqueIds;
    private Configuration yarnConfiguration;

    private DataContainer dataContainer;
    HashMap<String, Attribute> attributeMap;
    int processedRecord = 0;
    int errorRecord = 0;
    int duplicateRecord = 0;
    Integer[] result = new Integer[3];

    private CSVPrinter errorRecordPrinter;
    private CSVPrinter duplicateRecordPrinter;
    private Map<String, String> errorMap;
    private Map<String, String> duplicateMap;
    private String idColumnName;


    public VdbDataProcessCallable(Builder builder) {
        this.processorId = builder.getProcessorId();
        this.stop = builder.isStop();
        this.vdbValueConverter = builder.getVdbValueConverter();
        this.table = builder.getTable();
        this.fileQueue = builder.getFileQueue();
        this.extractPath = builder.getExtractPath();
        this.avroFileName = builder.getAvroFileName();
        this.needDedup = builder.getNeedDedup();
        this.uniqueIds = builder.getUniqueIds();
        this.yarnConfiguration = builder.getYarnConfiguration();
    }

    @Override
    public Integer[] call() throws Exception {
        log.info("Start Vdb data processor: " + processorId);
        long startTime = System.currentTimeMillis();
        intialize();
        while (!fileQueue.isEmpty() || !stop) {
            String dataFileName = fileQueue.poll();
            if (dataFileName == null) {
                Thread.sleep(1000);
            } else {
                log.info("Start process file " + dataFileName);
                VdbQueryDataResult vdbQueryDataResult = JsonUtils.deserialize(
                        new FileInputStream(new File(dataFileName)), VdbQueryDataResult.class);
                processedRecord += appendGenericRecord(vdbQueryDataResult);
            }
        }
        endProcess();
        log.info(String.format("Processor %s takes %d ms to process data.",
                processorId, System.currentTimeMillis() - startTime));
        return result;
    }

    private void intialize() {
        dataContainer = new DataContainer(vdbValueConverter, table, true);
        attributeMap = new HashMap<>();
        idColumnName = null;
        for (Attribute attr : table.getAttributes()) {
            attributeMap.put(attr.getSourceAttrName(), attr);
            if (attr.getInterfaceName() != null && attr.getInterfaceName().equals(InterfaceName.Id)) {
                idColumnName = attr.getName();
            }
        }
        errorMap = new HashMap<>();
        if (needDedup && idColumnName != null) {
            duplicateMap = new HashMap<>();
        } else {
            needDedup = false;
        }
        try {
            errorRecordPrinter = new CSVPrinter(new FileWriter(processorId + ERROR_FILE),
                    LECSVFormat.format.withHeader("ColumnName", "ErrorMessage"));
            if (needDedup) {
                duplicateRecordPrinter = new CSVPrinter(new FileWriter(processorId + DUPLICATE_FILE),
                        LECSVFormat.format.withHeader("ColumnName", "ErrorMessage"));
            }
        } catch (IOException e) {
            log.error("Can not create csv printer: " + e.getMessage());
        }
    }

    private void endProcess() {
        dataContainer.endContainer();
        copyToHdfs(dataContainer, extractPath + "/" + avroFileName);
        result[0] = processedRecord;
        result[1] = errorRecord;
        result[2] = duplicateRecord;
        copyCSVReport();

    }

    private void copyCSVReport() {
        try {
            errorRecordPrinter.close();
            if (errorRecord > 0) {
                HdfsUtils.copyLocalToHdfs(yarnConfiguration, processorId + ERROR_FILE, extractPath + "/" + ERROR_FILE);
            }
            if (needDedup && duplicateRecord > 0) {
                HdfsUtils.copyLocalToHdfs(yarnConfiguration, processorId + DUPLICATE_FILE, extractPath + "/" + DUPLICATE_FILE);
            }
        } catch (IOException e) {
            log.error("Error copy csv report file to HDFS!");
        }
    }


    private boolean copyToHdfs(DataContainer dataContainer, String filePath) {
        int retries = 0;
        Exception exception = null;
        boolean fileCopy = true;
        log.info(String.format("Copy data container file to HDFS path %s", filePath));
        do {
            try {
                retries++;
                exception = null;
                InputStream dataInputStream = new FileInputStream(dataContainer.getLocalDataFile());
                HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, dataInputStream, filePath);
            } catch (FileNotFoundException e) {
                log.error(String.format("Cannot find the data container file. Exception: %s, retry attempt = %d",
                        e.toString(), retries));
                fileCopy = false;
            } catch (IOException e) {
                log.error(String.format("Cannot write stream to Hdfs. Exception: %s, retry attempt = %d", e.toString(),
                        retries));
                exception = e;
            }
        } while (exception != null && retries <= MAX_RETRIES);

        if (exception != null && retries > MAX_RETRIES) {
            throw new RuntimeException(
                    String.format("Cannot write stream to Hdfs. Exception: %s", exception.toString()));
        }
        return fileCopy;
    }

    private int appendGenericRecord(VdbQueryDataResult vdbQueryDataResult) {
        int rowSize = vdbQueryDataResult.getColumns().get(0).getValues().size();
        int rowsAppend = 0;
        for (int i = 0; i < rowSize; i++) {
            if (validateRecord(vdbQueryDataResult, i)) {
                dataContainer.newRecord();
                for (VdbQueryResultColumn column : vdbQueryDataResult.getColumns()) {
                    if (attributeMap.containsKey(column.getColumnName())) {
                        Attribute attr = attributeMap.get(column.getColumnName());
                        dataContainer.setValueForAttribute(attr, column.getValues().get(i));
                    }
                }
                dataContainer.endRecord();
                rowsAppend++;
            } else {
                if (errorMap.size() > 0) {
                    handleError();
                }
                if (needDedup && duplicateMap.size() > 0) {
                    handleDuplicate();
                }
            }
        }
        return rowsAppend;
    }

    private void handleError() {
        try {
            for (Map.Entry<String, String> dupEntry : errorMap.entrySet()) {
                errorRecord++;
                errorRecordPrinter.printRecord(dupEntry.getKey(), dupEntry.getValue());
            }
            errorRecordPrinter.flush();
            errorMap.clear();
        } catch (IOException e) {
            log.error("Error write error info to CSV file!");
        }
    }

    private void handleDuplicate() {
        if (needDedup) {
            try {
                for (Map.Entry<String, String> dupEntry : duplicateMap.entrySet()) {
                    duplicateRecord++;
                    duplicateRecordPrinter.printRecord(dupEntry.getKey(), dupEntry.getValue());
                }
                duplicateRecordPrinter.flush();
                duplicateMap.clear();
            } catch (IOException e) {
                log.error("Error write duplicate info to CSV file!");
            }
        }
    }

    private boolean validateRecord(VdbQueryDataResult vdbQueryDataResult, int index) {
        boolean result = true;
        for (VdbQueryResultColumn column : vdbQueryDataResult.getColumns()) {
            if (attributeMap.containsKey(column.getColumnName())) {
                if (!attributeMap.get(column.getColumnName()).isNullable()) {
                    if (StringUtils.isEmpty(column.getValues().get(index))) { //check null value column
                        if (attributeMap.get(column.getColumnName()).getDefaultValueStr() != null) {
                            column.getValues()
                                    .set(index, attributeMap.get(column.getColumnName()).getDefaultValueStr());
                        } else {
                            result = false;
                            String record = getVdbRecord(vdbQueryDataResult, index);
                            log.error(String.format("Missing required field: %s. Record values: %s",
                                    attributeMap.get(column.getColumnName()).getName(), record));

                            errorMap.put(column.getColumnName(),
                                    String.format("Missing required field: %s. Record values: %s",
                                            attributeMap.get(column.getColumnName()).getName(), record));
                            break;
                        }
                    }
                }
                if (needDedup) {
                    if (idColumnName.equals(attributeMap.get(column.getColumnName()).getName())) {
                        String id = column.getValues().get(index);
                        if (uniqueIds.contains(id)) {
                            result = false;
                            duplicateMap.put(column.getColumnName(), String.format("The id %s is duplicated.", id));
                            break;
                        } else {
                            uniqueIds.add(id);
                        }
                    }
                }
            }
        }
        return result;
    }

    private String getVdbRecord(VdbQueryDataResult vdbQueryDataResult, int index) {
        String result = "";
        for (VdbQueryResultColumn column : vdbQueryDataResult.getColumns()) {
            result += column.getValues().get(index);
            result += ",";
        }
        if (!StringUtils.isEmpty(result)) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }


    public void setStop(boolean stop) {
        this.stop = stop;
    }

    public void addDataFile(String fileName) {
        this.fileQueue.offer(fileName);
    }

    public static class Builder {
        private String processorId;
        private boolean stop;
        private VdbValueConverter vdbValueConverter;
        private Table table;
        private Queue<String> fileQueue;
        private String extractPath;
        private String avroFileName;
        private boolean needDedup;
        private Set<String> uniqueIds;
        private Configuration yarnConfiguration;

        public Builder () {

        }

        public Builder processorId(String processorId) {
            this.processorId = processorId;
            return this;
        }

        public Builder stop(boolean stop) {
            this.stop = stop;
            return this;
        }

        public Builder vdbValueConverter(VdbValueConverter vdbValueConverter) {
            this.vdbValueConverter = vdbValueConverter;
            return this;
        }

        public Builder table(Table table) {
            this.table = table;
            return this;
        }

        public Builder fileQueue(Queue<String> fileQueue) {
            this.fileQueue = fileQueue;
            return this;
        }

        public Builder extractPath(String extractPath) {
            this.extractPath = extractPath;
            return this;
        }

        public Builder avroFileName(String avroFileName) {
            this.avroFileName = avroFileName;
            return this;
        }

        public Builder needDedup(boolean needDedup) {
            this.needDedup = needDedup;
            return this;
        }

        public Builder uniqueIds(Set<String> uniqueIds) {
            this.uniqueIds = uniqueIds;
            return this;
        }

        public Builder yarnConfiguration(Configuration yarnConfiguration) {
            this.yarnConfiguration = yarnConfiguration;
            return this;
        }

        public String getProcessorId() {
            return processorId;
        }

        public boolean isStop() {
            return stop;
        }

        public VdbValueConverter getVdbValueConverter() {
            return vdbValueConverter;
        }

        public Table getTable() {
            return table;
        }

        public Queue<String> getFileQueue() {
            return fileQueue;
        }

        public String getExtractPath() {
            return extractPath;
        }

        public String getAvroFileName() {
            return avroFileName;
        }

        public boolean getNeedDedup() {
            return needDedup;
        }

        public Set<String> getUniqueIds() {
            return uniqueIds;
        }

        public Configuration getYarnConfiguration() {
            return yarnConfiguration;
        }
    }
}

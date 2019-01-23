package com.latticeengines.serviceflows.workflow.export;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

@Component("exportData")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportData extends BaseExportData<ExportStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportData.class);
    private static final String MAPPED_FIELD_PREFIX = "user_";
    private static final String DATE_FORMAT = "M/d/yyyy";
    private static final String TIME_ZONE = "America/Los_Angeles";

    @Override
    public void execute() {
        log.info("Inside ExportData execute()");
        if ("true".equals(getStringValueFromContext(SKIP_EXPORT_DATA))) {
            log.info("Skip flag is set, skip export.");
            cleanupContext();
            return;
        }
        exportData();

        if (configuration.isExportMergedFile()
                || StringUtils.isNotBlank(getStringValueFromContext(EXPORT_MERGE_FILE_NAME))) {
            if (configuration.getExportFormat().equals(ExportFormat.CSV)) {
                mergeCSVFiles();
            }
        }
        cleanupContext();
    }

    private void cleanupContext() {
        removeObjectFromContext(EXPORT_TABLE_NAME);
        removeObjectFromContext(EXPORT_INPUT_PATH);
        removeObjectFromContext(EXPORT_OUTPUT_PATH);
        removeObjectFromContext(SKIP_EXPORT_DATA);
        removeObjectFromContext(EXPORT_MERGE_FILE_NAME);
        removeObjectFromContext(EXPORT_MERGE_FILE_PATH);
    }

    private void mergeCSVFiles() {
        String tenant = configuration.getCustomerSpace().toString();
        log.info("Tenant=" + tenant);
        Map<String, Attribute> importedAttributes = configuration.getNameToAttributeMap();
        log.info("Imported attributes=" + JsonUtils.serialize(importedAttributes));
        String mergeToPath = configuration.getExportTargetPath();
        if (StringUtils.isNotBlank(getStringValueFromContext(EXPORT_MERGE_FILE_PATH))) {
            mergeToPath = getStringValueFromContext(EXPORT_MERGE_FILE_PATH);
        }
        log.info("MergeTo=" + mergeToPath);

        String mergedFileName = configuration.getMergedFileName();
        if (StringUtils.isNotBlank(getStringValueFromContext(EXPORT_MERGE_FILE_NAME))) {
            mergedFileName = getStringValueFromContext(EXPORT_MERGE_FILE_NAME);
        }
        putStringValueInContext(MERGED_FILE_NAME, mergedFileName);
        log.info("MergedFileName=" + mergedFileName);

        try {
            String pathToLookFor = mergeToPath.substring(0, mergeToPath.lastIndexOf('/'));
            String filePrefix = mergedFileName.substring(0, mergedFileName.lastIndexOf(".csv"));
            List<String> csvFiles = HdfsUtils.getFilesForDir(yarnConfiguration, pathToLookFor,
                    filePrefix + "_.*.csv$");
            log.info("HDFS CSV files=" + JsonUtils.serialize(csvFiles));
            String localCsvFilesPath = "csvFiles";
            File localCsvDir = new File(localCsvFilesPath);
            if (!localCsvDir.exists()) {
                if (!localCsvDir.mkdir()) {
                    throw new IOException(String.format("Cannot create local path %s", localCsvFilesPath));
                }
            }

            for (String file : csvFiles) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, file, localCsvFilesPath);
            }

            File localOutputCSV = new File(localCsvFilesPath, mergedFileName);
            log.info("Local output CSV=" + localOutputCSV.toString());

            CSVWriter writer = new CSVWriter(new FileWriter(localOutputCSV), CSVWriter.DEFAULT_SEPARATOR,
                    CSVWriter.DEFAULT_QUOTE_CHARACTER);

            File[] files = localCsvDir.listFiles(
                    file -> file.getName().matches("\\w+_part-[\\w\\d-]+.csv$|.*-p-\\d+.csv$"));

            if (files == null) {
                throw new RuntimeException("Cannot list files in dir " + localCsvDir);
            }

            boolean hasHeader = false;
            for (File file : files) {
                log.info("Merging file " + file.getName());
                CSVReader csvReader = new CSVReader(new FileReader(file));
                List<String[]> records = csvReader.readAll();
                log.info(String.format("There are %d rows in file %s.", records.size(), file.getName()));
                String[] header = records.get(0);
                boolean needRemapFieldNames = file.getName().toLowerCase().contains("orphan")
                        && MapUtils.isNotEmpty(importedAttributes);
                if (needRemapFieldNames) {
                    log.info("Remap field names.");
                    Map<String, Integer> headerPosMap = buildPositionMap(Arrays.asList(header));
                    log.info("Header positionMap=" + JsonUtils.serialize(headerPosMap));
                    List<String> displayNames = importedAttributes.entrySet().stream()
                            .map(entry -> normalizeFieldName(entry.getValue().getDisplayName()))
                            .collect(Collectors.toList());
                    log.info("DisplayName positionMap=" + JsonUtils.serialize(displayNames));
                    Map<String, Integer> displayNamePosMap = buildPositionMap(displayNames);
                    String[] displayNamesAsArr = toOrderedArray(displayNamePosMap);
                    if (!hasHeader) {
                        writer.writeNext(displayNamesAsArr);
                        hasHeader = true;
                    }
                    if (records.size() > 1) {
                        records.subList(1, records.size()).forEach(record -> {
                            String[] newRecord = generateNewRecord(record, displayNamesAsArr.length,
                                    headerPosMap, displayNamePosMap, importedAttributes);
                            writer.writeNext(newRecord);
                        });
                    }
                } else {
                    log.info("Use field names as-is.");
                    if (!hasHeader) {
                        writer.writeNext(header);
                        hasHeader = true;
                    }
                    if (records.size() > 1) {
                        writer.writeAll(records.subList(1, records.size()));
                    }
                }
                csvReader.close();
            }
            log.info("Finished for loops.");
            writer.flush();
            writer.close();
            log.info("Start copying file from local to hdfs.");
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localOutputCSV.getPath(), mergeToPath);
            log.info(String.format("Copied merged CSV file from local %s to HDFS %s", localOutputCSV.getPath(),
                    mergeToPath));
            FileUtils.deleteDirectory(localCsvDir);
            log.info("Done merging CSV files.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected String getTableName() {
        String tableName = getStringValueFromContext(EXPORT_TABLE_NAME);
        if (tableName == null) {
            tableName = configuration.getTableName();
        }
        return tableName;
    }

    protected String getExportInputPath() {
        String inputPath = getStringValueFromContext(EXPORT_INPUT_PATH);
        return StringUtils.isNotBlank(inputPath) ? inputPath : null;
    }

    protected String getExportOutputPath() {
        String outputPath = getStringValueFromContext(EXPORT_OUTPUT_PATH);
        return StringUtils.isNotBlank(outputPath) ? outputPath : null;
    }

    private Map<String, Integer> buildPositionMap(List<String> fields) {
        Map<String, Integer> positionMap = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            positionMap.put(fields.get(i), i);
        }
        return positionMap;
    }

    @VisibleForTesting
    @SuppressWarnings("unchecked")
    public String[] generateNewRecord(String[] oldRecord, int fieldCount,
                                      Map<String, Integer> namePosMap, Map<String, Integer> displayNamePosMap,
                                      Map<String, Attribute> attributeMap) {
        String[] result = new String[fieldCount];
        namePosMap.forEach((name, position) -> {
            String data = oldRecord[position];
            if (attributeMap.containsKey(name)) {
                String displayName = attributeMap.get(name).getDisplayName();
                LogicalDataType logicalDataType = attributeMap.get(name).getLogicalDataType();
                if (displayNamePosMap.containsKey(displayName)) {
                    if (logicalDataType == LogicalDataType.Timestamp) {
                        data = toDateAsString(data);
                    }
                    result[displayNamePosMap.get(displayName)] = data;
                }
            }

            // expand CustomTrxField
            if (name.equalsIgnoreCase(InterfaceName.CustomTrxField.name())) {
                Map<String, Object> customFields = JsonUtils.deserialize(data, Map.class);
                customFields.forEach((rawFieldName, fieldValue) -> {
                    String fieldName = normalizeFieldName(rawFieldName);
                    if (displayNamePosMap.containsKey(fieldName)) {
                        result[displayNamePosMap.get(fieldName)] = String.valueOf(fieldValue);
                    }
                });
            }
        });
        return result;
    }

    @VisibleForTesting
    public String[] toOrderedArray(Map<String, Integer> positionMap) {
        String[] arr = new String[positionMap.size()];
        positionMap.forEach((field, index) -> arr[index] = field);
        return arr;
    }

    private String normalizeFieldName(String displayName) {
        String result = displayName;
        if (result.startsWith(MAPPED_FIELD_PREFIX)) {
            result = result.substring(MAPPED_FIELD_PREFIX.length()).replace('_', ' ');
        }
        return result;
    }

    private String toDateAsString(String timestamp) {
        SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
        formatter.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
        Date date = Date.from(Instant.ofEpochMilli(Long.valueOf(timestamp)));
        return formatter.format(date);
    }
}

package com.latticeengines.serviceflows.workflow.export;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

public abstract class BaseExportData<T extends ExportStepConfiguration> extends BaseWorkflowStep<T> {

    private static final String MAPPED_FIELD_PREFIX = "user_";
    private static final String DATE_FORMAT = "M/d/yyyy";
    private static final String TIME_ZONE = "UTC";

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private EaiProxy eaiProxy;

    protected String exportData() {
        ExportConfiguration exportConfig = setupExportConfig();
        AppSubmission submission = eaiProxy.submitEaiJob(exportConfig);
        putStringValueInContext(EXPORT_DATA_APPLICATION_ID, submission.getApplicationIds().get(0));
        waitForAppId(submission.getApplicationIds().get(0));
        return exportConfig.getExportTargetPath();
    }

    private ExportConfiguration setupExportConfig() {
        ExportConfiguration exportConfig = new ExportConfiguration();
        exportConfig.setExportFormat(configuration.getExportFormat());
        exportConfig.setExportDestination(configuration.getExportDestination());
        exportConfig.setCustomerSpace(configuration.getCustomerSpace());
        exportConfig.setUsingDisplayName(configuration.getUsingDisplayName());
        exportConfig.setExclusionColumns(getExclusionColumns());
        exportConfig.setInclusionColumns(getInclusionColumns());
        exportConfig.setTable(retrieveTable());
        exportConfig.setExportInputPath(getExportInputPath());
        Map<String, String> properties = configuration.getProperties();
        if (StringUtils.isNotBlank(getExportOutputPath())) {
            exportConfig.setExportTargetPath(getExportOutputPath());
        } else if (properties.containsKey(ExportProperty.TARGET_FILE_NAME)) {
            String targetPath = PathBuilder
                    .buildDataFileExportPath(CamilleEnvironment.getPodId(), configuration.getCustomerSpace())
                    .append(properties.get(ExportProperty.TARGET_FILE_NAME)).toString();
            exportConfig.setExportTargetPath(targetPath);
            saveOutputValue(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH, targetPath);
        }
        for (String propertyName : configuration.getProperties().keySet()) {
            exportConfig.setProperty(propertyName, configuration.getProperties().get(propertyName));
        }
        return exportConfig;
    }

    private Table retrieveTable() {
        String tableName = getTableName();
        return metadataProxy.getTable(configuration.getCustomerSpace().toString(), tableName);
    }

    protected String getExclusionColumns() {
        return null;
    }

    protected String getInclusionColumns() {
        return null;
    }

    protected abstract String getTableName();
    protected abstract String getExportInputPath();
    protected abstract String getExportOutputPath();

    protected List<String> mergeCSVFiles() {
        return mergeCSVFiles(false);
    }

    /*-
     * merged filename in hdfs:
     * keepMergedFileName=true -> will use configuration.getMergedFileName()
     * keepMergedFileName=false -> will use mergeToPath if it's a file path
     */
    protected List<String> mergeCSVFiles(boolean keepMergedFileName) {
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
            int lastSlashPos = mergeToPath.lastIndexOf('/');
            String pathToLookFor = mergeToPath.substring(0, lastSlashPos);
            String filePrefix = mergeToPath.substring(lastSlashPos + 1);
            String dstPath = keepMergedFileName ? pathToLookFor : mergeToPath;
            log.info("PathToLookFor=" + pathToLookFor);
            log.info("FilePrefix=" + filePrefix);
            List<String> csvFiles = HdfsUtils.getFilesForDir(yarnConfiguration, pathToLookFor, filePrefix + "_.*.csv$");
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
                if (!keepMergedFileName) {
                    HdfsUtils.moveFile(yarnConfiguration, file, mergeToPath);
                }
            }

            File localOutputCSV = new File(localCsvFilesPath, mergedFileName);
            log.info("Local output CSV=" + localOutputCSV.toString());

            CSVWriter writer = new CSVWriter(new FileWriter(localOutputCSV), CSVWriter.DEFAULT_SEPARATOR,
                    CSVWriter.DEFAULT_QUOTE_CHARACTER);

            File[] files = localCsvDir
                    .listFiles(file -> file.getName().matches("\\w+_part-[\\w\\d-]+.csv$|.*-p-\\d+.csv$"));

            if (files == null) {
                throw new RuntimeException("Cannot list files in dir " + localCsvDir);
            }

            log.info("Local CSV files=" + JsonUtils.serialize(files));

            boolean hasHeader = false;
            for (File file : files) {
                log.info("Merging file " + file.getName());
                CSVReader csvReader = new CSVReader(new FileReader(file));
                List<String[]> records = csvReader.readAll();
                log.info(String.format("There are 1 header row, %d data row(s) in file %s.", records.size() - 1,
                        file.getName()));
                String[] header = records.get(0);
                boolean needRemapFieldNames = (file.getName().toLowerCase().contains("orphan")
                        || file.getName().toLowerCase().contains("unmatched"))
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
                            String[] newRecord = generateNewRecord(record, displayNamesAsArr.length, headerPosMap,
                                    displayNamePosMap, importedAttributes);
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
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localOutputCSV.getPath(), dstPath);
            log.info(String.format("Copied merged CSV file from local %s to HDFS %s", localOutputCSV.getPath(),
                    dstPath));
            if (keepMergedFileName) {
                // remove original files
                csvFiles.forEach(path -> {
                    try {
                        HdfsUtils.rmdir(yarnConfiguration, path);
                    } catch (IOException e) {
                        log.error("Failed to delete path {}, error = {}", path, e);
                    }
                });
                log.info("Removing original files {} after merged", csvFiles);
            }
            FileUtils.deleteDirectory(localCsvDir);
            log.info("Done merging CSV files.");
            return csvFiles;
        } catch (Exception e) {
            log.warn("Failed to merge csv files", e);
            return Collections.emptyList();
        }
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
    public String[] generateNewRecord(String[] oldRecord, int fieldCount, Map<String, Integer> namePosMap,
            Map<String, Integer> displayNamePosMap, Map<String, Attribute> attributeMap) {
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

    private String[] toOrderedArray(Map<String, Integer> positionMap) {
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

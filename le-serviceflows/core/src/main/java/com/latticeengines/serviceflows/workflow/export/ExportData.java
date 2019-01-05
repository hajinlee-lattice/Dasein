package com.latticeengines.serviceflows.workflow.export;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

@Component("exportData")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportData extends BaseExportData<ExportStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportData.class);

    @Inject
    private MetadataProxy metadataProxy;

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

    protected void mergeCSVFiles() {
        String tenant = configuration.getCustomerSpace().toString();
        log.info("Tenant=" + tenant);
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
            List<String> csvFiles = HdfsUtils.getFilesForDir(yarnConfiguration, mergeToPath, ".*.csv$");
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

            File[] files = localCsvDir.listFiles(file -> {
                return file.getName().matches("\\w+_part-[\\w\\d-]+.csv$|.*-p-\\d+.csv$");
            });

            if (files == null) {
                throw new RuntimeException("Cannot list files in dir " + localCsvDir);
            }

            boolean hasHeader = false;
            for (File file : files) {
                log.info("Merging file " + file.getName());
                CSVReader csvReader = new CSVReader(new FileReader(file));
                List<String[]> records = csvReader.readAll();
                String[] header = records.get(0);
                log.info(String.format("There are %d rows in file %s.", records.size(), file.getName()));
                boolean useSourceTable = false;
                if (file.getName().toLowerCase().contains("orphantransactions"))  {
                    Table sourceTable = getSourceTable(tenant, "transaction");
                    if (sourceTable != null) {
                        log.info("Source table=" + sourceTable.getName());
                        useSourceTable = true;
                        List<String> sourceFields = getSourceFields(tenant, sourceTable);
                        log.info(String.format("Fields in source table %s: %s",
                                sourceTable.getName(), JsonUtils.serialize(sourceFields)));
                        Map<String, Integer> sourcePositionMap = buildPositionMap(sourceFields);
                        Map<String, Integer> headerPositionMap = buildPositionMap(Arrays.asList(header));
                        String[] sourceFieldAsHeader = sourceFields.toArray(new String[0]);
                        if (!hasHeader) {
                            writer.writeNext(sourceFieldAsHeader);
                            hasHeader = true;
                        }
                        if (records.size() > 1) {
                            records.subList(1, records.size()).forEach(record -> {
                                String[] newRecord = generateNewRecord(record, sourceFieldAsHeader.length,
                                        headerPositionMap, sourcePositionMap);
                                writer.writeNext(newRecord);
                            });
                        }
                    }
                } else if (file.getName().toLowerCase().contains("orphancontacts")) {
                    Table sourceTable = getSourceTable(tenant, "contact");
                    if (sourceTable != null) {
                        log.info("Source table=" + sourceTable.getName());
                        useSourceTable = true;
                        List<String> sourceFields = getSourceFields(tenant, sourceTable);
                        log.info(String.format("Fields in source table %s: %s",
                                sourceTable.getName(), JsonUtils.serialize(sourceFields)));
                        Map<String, Integer> sourcePositionMap = buildPositionMap(sourceFields);
                        Map<String, Integer> headerPositionMap = buildPositionMap(Arrays.asList(header));
                        String[] sourceFieldAsHeader = sourceFields.toArray(new String[0]);
                        if (!hasHeader) {
                            writer.writeNext(sourceFieldAsHeader);
                            hasHeader = true;
                        }
                        if (records.size() > 1) {
                            records.subList(1, records.size()).forEach(record -> {
                                String[] newRecord = generateNewRecord(record, sourceFieldAsHeader.length,
                                        headerPositionMap, sourcePositionMap);
                                writer.writeNext(newRecord);
                            });
                        }
                    }
                }

                if (!useSourceTable) {
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

    private Table getSourceTable(String tenant, String tableInterpretation) {
        List<String> tableNames = metadataProxy.getTableNames(tenant);
        if (tableNames == null) {
            return null;
        }
        for (String tableName : tableNames) {
            if (tableName.toLowerCase().startsWith("sourcefile")) {
                Table sourceTable = metadataProxy.getTable(tenant, tableName);
                if (sourceTable.getInterpretation().equalsIgnoreCase(tableInterpretation)
                        && sourceTable.getTableType() == TableType.IMPORTTABLE) {
                    return sourceTable;
                }
            }
        }
        return null;
    }

    private List<String> getSourceFields(String tenant, Table sourceTable) {
        if (sourceTable == null) {
            throw new RuntimeException("Source table is null. Tenant=" + tenant);
        }

        List<Attribute> sourceAttributes = metadataProxy.getTableAttributes(tenant, sourceTable.getName(), null);
        return sourceAttributes.stream().map(Attribute::getDisplayName).collect(Collectors.toList());
    }

    private Map<String, Integer> buildPositionMap(List<String> fields) {
        Map<String, Integer> positionMap = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            positionMap.put(fields.get(i), i);
        }
        return positionMap;
    }

    @SuppressWarnings("unchecked")
    private String[] generateNewRecord(String[] oldRecord, int recordSize,
                                       Map<String, Integer> headerMap, Map<String, Integer> sourceMap) {
        String[] result = new String[recordSize];
        headerMap.forEach((field, index) -> {
            String data = oldRecord[index];
            // expand CustomTrxField
            if (field.equalsIgnoreCase(InterfaceName.CustomTrxField.name())) {
                Map<String, Object> customFields = JsonUtils.deserialize(data, Map.class);
                String fieldPrefix = "user_";
                customFields.forEach((rawFieldName, fieldValue) -> {
                    String fieldName = rawFieldName.substring(fieldPrefix.length()).replace('_', ' ');
                    if (sourceMap.containsKey(fieldName)) {
                        result[sourceMap.get(fieldName)] = String.valueOf(fieldValue);
                    }
                });
            }
            if (sourceMap.containsKey(field)) {
                result[sourceMap.get(field)] = data;
            }
        });
        return result;
    }
}

package com.latticeengines.serviceflows.workflow.export;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

@Component("exportData")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportData extends BaseExportData<ExportStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportData.class);

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
                String[] headerLine = records.get(0);
                List<String[]> dataLines = records.subList(1, records.size());

                // get index of TransactionDate field
                int transactionDateIndex = -1;
                for (int i = 0; i < headerLine.length; i++) {
                    if (headerLine[i].equalsIgnoreCase(InterfaceName.TransactionDate.name())) {
                        transactionDateIndex = i;
                        break;
                    }
                }

                Set<List<String>> recordsSet = new HashSet<>();
                for (String[] line : dataLines) {
                    if (transactionDateIndex > 0) {
                        line[transactionDateIndex] = toDefaultDateFormat(line[transactionDateIndex]);
                    }
                    recordsSet.add(Arrays.asList(line));
                }
                List<String[]> dataRecords = new ArrayList<>();
                recordsSet.forEach(record -> {
                    String[] dataArr = new String[record.size()];
                    dataRecords.add(dataArr);
                });
                if (!hasHeader) {
                    writer.writeNext(headerLine);
                    hasHeader = true;
                }
                if (dataRecords.size() > 1) {
                    writer.writeAll(dataRecords.subList(1, dataRecords.size()));
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

    private String toDefaultDateFormat(String sourceDateString) throws Exception {
        SimpleDateFormat srcFormat = new SimpleDateFormat(DateTimeUtils.DATE_ONLY_FORMAT_STRING);
        SimpleDateFormat dstFormat = new SimpleDateFormat("M/d/yyyy");  // default format (M/d/yyyy)
        return dstFormat.format(srcFormat.parse(sourceDateString));
    }

}

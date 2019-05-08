package com.latticeengines.cdl.workflow.steps.validations.service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.InputFileValidationServiceConfiguration;

public abstract class InputFileValidationService<T extends InputFileValidationServiceConfiguration> {
    private static Logger log = LoggerFactory.getLogger(InputFileValidationService.class);
    private static Map<String, InputFileValidationService<? extends InputFileValidationServiceConfiguration>> map = new HashMap<>();
    @Autowired
    protected Configuration yarnConfiguration;

    public abstract void validate(T inputFileValidationServiceConfiguration);


    public InputFileValidationService(String serviceName) {
        map.put(serviceName, this);
    }

    public static InputFileValidationService<? extends InputFileValidationServiceConfiguration> getValidationService(
            String serviceName) {
        return map.get(serviceName);
    }


    protected static String getString(GenericRecord record, String field) {
        String value;
        try {
            value = record.get(field).toString();
        } catch (Exception e) {
            value = null;
        }
        return value;
    }

    protected void writeErrorFile(String filePath, Map<String, String> errorMessages) {

        CSVFormat format = LECSVFormat.format;
        // copy error file if file exists, remove hdfs
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, filePath)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, filePath, ImportProperty.ERROR_FILE);
                HdfsUtils.rmdir(yarnConfiguration, filePath);
            } else {
                format = format.withHeader(ImportProperty.ERROR_HEADER);
            }
        } catch (IOException e) {
            log.info("Error when copying file to local");
        }
        // append error message to error file
        try (CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(ImportProperty.ERROR_FILE, true), format)) {
            for (Map.Entry<String, String> entry : errorMessages.entrySet()) {
                csvFilePrinter.printRecord(entry.getKey(), "", entry.getValue());
            }
            csvFilePrinter.close();
        } catch (IOException ex) {
            log.info("Error when writing error message to error file");
        }

        // copy error file back to hdfs, remove local error.csv
        try {
            HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, ImportProperty.ERROR_FILE, filePath);
            FileUtils.forceDelete(new File(ImportProperty.ERROR_FILE));
        } catch (IOException e) {
            log.info("Error when copying file to hdfs");
        }
    }

    protected static String getPath(String avroDir) {
        log.info("Get avro path input " + avroDir);
        if (!avroDir.endsWith(".avro")) {
            return avroDir;
        } else {
            String[] dirs = avroDir.trim().split("/");
            avroDir = "";
            for (int i = 0; i < (dirs.length - 1); i++) {
                log.info("Get avro path dir " + dirs[i]);
                if (!dirs[i].isEmpty()) {
                    avroDir = avroDir + "/" + dirs[i];
                }
            }
        }
        log.info("Get avro path output " + avroDir);
        return avroDir;
    }
}

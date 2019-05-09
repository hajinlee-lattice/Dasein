package com.latticeengines.cdl.workflow.steps.validations.service;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.InputFileValidationConfiguration;

public abstract class InputFileValidationService<T extends InputFileValidationConfiguration> {
    private static Logger log = LoggerFactory.getLogger(InputFileValidationService.class);
    private static Map<String, InputFileValidationService<? extends InputFileValidationConfiguration>> map = new HashMap<>();
    @Autowired
    protected Configuration yarnConfiguration;

    public abstract void validate(T inputFileValidationServiceConfiguration);


    public InputFileValidationService(String serviceName) {
        map.put(serviceName, this);
    }

    public static InputFileValidationService<? extends InputFileValidationConfiguration> getValidationService(
            String serviceName) {
        return map.get(serviceName);
    }


    protected static String getFieldValue(GenericRecord record, String field) {
        String value;
        try {
            value = record.get(field).toString();
        } catch (Exception e) {
            value = null;
        }
        return value;
    }

    protected void writeErrorFile(String lineId, String errorMessage, CSVFormat format) {

        // append error message to error file
        try (CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(ImportProperty.ERROR_FILE, true), format)) {
            csvFilePrinter.printRecord(lineId, "", errorMessage);
        } catch (IOException ex) {
            log.info("Error when writing error message to error file");
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

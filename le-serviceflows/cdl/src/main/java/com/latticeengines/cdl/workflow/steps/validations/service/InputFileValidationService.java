package com.latticeengines.cdl.workflow.steps.validations.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.InputFileValidationConfiguration;

public abstract class InputFileValidationService<T extends InputFileValidationConfiguration> {
    private static Logger log = LoggerFactory.getLogger(InputFileValidationService.class);
    private static Map<String, InputFileValidationService<? extends InputFileValidationConfiguration>> map = new HashMap<>();
    protected static final List<Character> invalidChars = Arrays.asList('/', '&');
    @Autowired
    protected Configuration yarnConfiguration;

    public abstract long validate(T inputFileValidationServiceConfiguration, List<String> processedLines);


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


    protected static String getPath(String avroDir) {
        log.info("Get avro path input " + avroDir);
        if (!avroDir.endsWith(".avro")) {
            return avroDir;
        } else {
            String[] dirs = avroDir.trim().split("/");
            avroDir = "";
            for (int i = 0; i < (dirs.length - 1); i++) {
                if (!dirs[i].isEmpty()) {
                    avroDir = avroDir + "/" + dirs[i];
                }
            }
        }
        log.info("Get avro path output " + avroDir);
        return avroDir;
    }
}

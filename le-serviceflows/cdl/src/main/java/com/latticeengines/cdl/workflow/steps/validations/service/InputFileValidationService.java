package com.latticeengines.cdl.workflow.steps.validations.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.InputFileValidationConfiguration;
import com.latticeengines.domain.exposed.util.ProductUtils;

public abstract class InputFileValidationService<T extends InputFileValidationConfiguration> {
    private static Map<String, InputFileValidationService<? extends InputFileValidationConfiguration>> map = new HashMap<>();
    protected static final List<Character> invalidChars = Arrays.asList('/', '&');
    @Autowired
    protected Configuration yarnConfiguration;

    public abstract long validate(T inputFileValidationServiceConfiguration, List<String> processedLines,
                                  StringBuilder stastistics);


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

    protected static String getFieldDisplayName(GenericRecord record, String field, String defaultName) {
        if (record == null) {
            return defaultName;
        }
        Schema schema = record.getSchema();
        if (schema == null) {
            return defaultName;
        }
        Schema.Field schemaField = schema.getField(field);
        if (schemaField == null) {
            return defaultName;
        }
        String displayName = schemaField.getProp("displayName");
        if (StringUtils.isEmpty(displayName)) {
            return defaultName;
        } else {
            return displayName;
        }
    }

    protected static String getPath(String avroDir) {
       return ProductUtils.getPath(avroDir);
    }
}

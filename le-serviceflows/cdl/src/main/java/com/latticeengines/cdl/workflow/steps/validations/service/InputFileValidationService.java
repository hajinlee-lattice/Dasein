package com.latticeengines.cdl.workflow.steps.validations.service;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;


import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.InputFileValidationConfiguration;
import com.latticeengines.domain.exposed.util.ProductUtils;

public abstract class InputFileValidationService<T extends InputFileValidationConfiguration> {
    private static Map<Class<? extends InputFileValidationConfiguration>, InputFileValidationService<? extends InputFileValidationConfiguration>> map =
            new HashMap<>();

    protected static final List<Character> invalidChars = Arrays.asList('/', '&');

    @Autowired
    protected Configuration yarnConfiguration;

    protected static final String PATH_SEPARATOR = "/";


    private static final Logger log = LoggerFactory.getLogger(InputFileValidationService.class);

    public abstract long validate(T inputFileValidationServiceConfiguration, List<String> processedRecords,
                                  StringBuilder statistics);

    @SuppressWarnings("unchecked")
    public InputFileValidationService() {
        map.put((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], this);
    }

    public static InputFileValidationService<? extends InputFileValidationConfiguration> getValidationService(
            Class<? extends InputFileValidationConfiguration> clz) {
        return map.get(clz);
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

    protected CSVFormat copyErrorFileToLocalIfExist(String errorFile) {
        CSVFormat format = LECSVFormat.format;
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, errorFile)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, errorFile, ImportProperty.ERROR_FILE);
                format = format.withSkipHeaderRecord();
            } else {
                format = format.withHeader(ImportProperty.ERROR_HEADER);
            }
        } catch (IOException e) {
            log.info("Error when copying error file to local");
        }
        return format;
    }

    protected void copyErrorFileBackToHdfs(String errorFile) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, errorFile)) {
                HdfsUtils.rmdir(yarnConfiguration, errorFile);
            }
            HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, ImportProperty.ERROR_FILE, errorFile);
            FileUtils.forceDelete(new File(ImportProperty.ERROR_FILE));
        } catch (IOException e) {
            log.info("Error when copying file to hdfs");
        }
    }

}

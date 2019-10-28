package com.latticeengines.cdl.workflow.steps.validations.service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;


import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.validator.URLValidator;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
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

    protected long handleRecords(CSVFormat format, List<String> pathList, List<String> processedRecords,
                                 InterfaceName interfaceName, URLValidator urlValidator) {
        long errorLine = 0L;
        String errMSG = null;
        if (InterfaceName.PathPattern.equals(interfaceName)) {
            errMSG = "invalidate path found in this row";
        } else if (InterfaceName.WebVisitPageUrl.equals(interfaceName)) {
            errMSG = "invalidate url found in this row";
        }

        try (CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(ImportProperty.ERROR_FILE, true), format)) {
            // iterate through all files, remove all illegal record row
            for (int i = 0; i < pathList.size(); i++) {
                String path = pathList.get(i);
                long errorInPath = 0L;
                try {
                    path = getPath(path);
                    log.info("begin dealing with path " + path);
                    List<String> avroFileList = HdfsUtils.getFilesByGlob(yarnConfiguration, path + "/*.avro");
                    for (String avroFile : avroFileList) {
                        boolean fileError = false;
                        String avroFileName = avroFile.substring(avroFile.lastIndexOf("/") + 1);
                        try(FileReader<GenericRecord> fileReader = AvroUtils.getAvroFileReader(yarnConfiguration,
                                new Path(avroFile))) {
                            Schema schema = fileReader.getSchema();
                            try (DataFileWriter<GenericRecord> dataFileWriter =
                                         new DataFileWriter<>(new GenericDatumWriter<>())) {
                                dataFileWriter.create(schema, new File(avroFileName));
                                // iterate through all records in avro files
                                for (GenericRecord record : fileReader) {
                                    boolean rowError = false;
                                    String pathStr = getFieldValue(record, interfaceName.name());
                                    if (StringUtils.isNotBlank(pathStr)) {
                                        // validate url legacy
                                        if (!urlValidator.isValidPath(pathStr)) {
                                            String lineId = getFieldValue(record, InterfaceName.InternalId.name());
                                            rowError = true;
                                            fileError = true;
                                            errorInPath++;
                                            errorLine++;
                                            csvFilePrinter.printRecord(lineId, "", errMSG);
                                        }
                                    }
                                    // if row is not error, write avro row to local file
                                    if (!rowError) {
                                        dataFileWriter.append(record);
                                    }
                                }
                            }

                        } catch (IOException e3) {
                            throw new RuntimeException(e3);
                        }
                        // if found file error, copy the local file to hdfs, then remove local avro file
                        if (fileError) {
                            if (HdfsUtils.fileExists(yarnConfiguration, avroFile)) {
                                HdfsUtils.rmdir(yarnConfiguration, avroFile);
                            }
                            HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, avroFileName, avroFile);
                        }
                        FileUtils.forceDelete(new File(avroFileName));
                    }
                } catch (IOException e2) {
                    throw new RuntimeException(e2);
                }
                // modify processed records if necessary
                if (errorInPath != 0L) {
                    long processed = Long.parseLong(processedRecords.get(i)) - errorInPath;
                    processedRecords.set(i, String.valueOf(processed));
                }
            }
        } catch (IOException e1) {
            log.info("Error when writing error message to error file");
        }
        return errorLine;
    }

}

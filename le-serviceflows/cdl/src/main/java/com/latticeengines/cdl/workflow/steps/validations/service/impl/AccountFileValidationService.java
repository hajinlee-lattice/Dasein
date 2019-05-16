package com.latticeengines.cdl.workflow.steps.validations.service.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.validations.service.InputFileValidationService;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.AccountFileValidationConfiguration;


@Component("accountFileValidationService")
@Lazy(value = false)
public class AccountFileValidationService
        extends InputFileValidationService<AccountFileValidationConfiguration> {


    private static final List<Character> invalidChars = Arrays.asList('/', '&');
    private static Logger log = LoggerFactory.getLogger(AccountFileValidationService.class);

    public AccountFileValidationService() {
        super(AccountFileValidationConfiguration.class.getSimpleName());
    }

    @Override
    public long validate(AccountFileValidationConfiguration accountFileValidationServiceConfiguration) {

        // check entity match, change name to transformed name
        boolean enableEntityMatch = accountFileValidationServiceConfiguration.isEnableEntityMatch();
        InterfaceName interfaceName = InterfaceName.AccountId;
        if (enableEntityMatch) {
            interfaceName = InterfaceName.CustomerAccountId;
        }

        long errorLine = 0L;
        List<String> pathList = accountFileValidationServiceConfiguration.getPathList();
        CSVFormat format = LECSVFormat.format;
        // copy error file if file exists
        String errorFile = getPath(pathList.get(0)) + "/" + ImportProperty.ERROR_FILE;
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, errorFile)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, errorFile, ImportProperty.ERROR_FILE);
            } else {
                format = format.withHeader(ImportProperty.ERROR_HEADER);
            }
        } catch (IOException e) {
            log.info("Error when copying error file to local");
        }
        try (CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(ImportProperty.ERROR_FILE, true), format)) {
            // iterate through all files, remove all illegal record row
            for (String path : pathList) {
                try {
                    path = getPath(path);
                    log.info("begin dealing with path " + path);
                    List<String> avroFileList = HdfsUtils.getFilesByGlob(yarnConfiguration, path + "/*.avro");
                    for (String avroFile : avroFileList) {
                        String avrofileName = avroFile.substring(avroFile.lastIndexOf("/") + 1);
                        boolean fileError = false;
                        try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(yarnConfiguration,
                                new Path(avroFile))) {
                            // create temp file in local
                            Schema schema = reader.getSchema();

                            try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(
                                    new GenericDatumWriter<>())) {
                                dataFileWriter.create(schema, new File(avrofileName));
                                // iterate through record in avro file

                                for (GenericRecord record : reader) {
                                    boolean rowError = false;
                                    String id = getFieldValue(record, interfaceName.name());
                                    if (StringUtils.isEmpty(id)) {
                                        id = getFieldValue(record, InterfaceName.Id.name());
                                    }
                                    if (StringUtils.isEmpty(id)) {
                                        log.info("Empty id is found from avro file");
                                        dataFileWriter.append(record);
                                        continue;
                                    }
                                    for (Character c : invalidChars) {
                                        if (id.indexOf(c) != -1) {
                                            String lineId = getFieldValue(record, InterfaceName.InternalId.name());
                                            String message = String.format(
                                                    "Invalid account id is found due to %s in %s.", c.toString(), id);
                                            csvFilePrinter.printRecord(lineId, "", message);
                                            rowError = true;
                                            fileError = true;
                                            errorLine++;
                                            break;
                                        }
                                    }
                                    if (!rowError) {
                                        dataFileWriter.append(record);
                                    }
                                }
                            }

                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        // record error in error.csv if not empty, copy the
                        // new generated avro file to hdfs
                        if (fileError) {
                            if (HdfsUtils.fileExists(yarnConfiguration, avroFile)) {
                                HdfsUtils.rmdir(yarnConfiguration, avroFile);
                            }
                            HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, avrofileName, avroFile);
                        }
                        FileUtils.forceDelete(new File(avrofileName));
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (IOException ex) {
            log.info("Error when writing error message to error file");
        }

        // copy error file back to hdfs, remove local error.csv
        if (errorLine != 0L) {
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
        return errorLine;
    }
}

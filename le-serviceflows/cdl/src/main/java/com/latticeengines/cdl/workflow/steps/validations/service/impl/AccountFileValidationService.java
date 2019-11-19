package com.latticeengines.cdl.workflow.steps.validations.service.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.EntityValidationSummary;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.AccountFileValidationConfiguration;


@Component("accountFileValidationService")
@Lazy(value = false)
public class AccountFileValidationService
        extends InputFileValidationService<AccountFileValidationConfiguration> {


    private static Logger log = LoggerFactory.getLogger(AccountFileValidationService.class);

    @Override
    public EntityValidationSummary validate(AccountFileValidationConfiguration accountFileValidationServiceConfiguration,
                                            List<String> processedRecords) {

        // check entity match, change name to transformed name
        boolean enableEntityMatch = accountFileValidationServiceConfiguration.isEnableEntityMatch();
        boolean enableEntityMatchGA = accountFileValidationServiceConfiguration.isEnableEntityMatchGA();
        InterfaceName interfaceName = InterfaceName.AccountId;
        if (enableEntityMatch || enableEntityMatchGA) {
            interfaceName = InterfaceName.CustomerAccountId;
        }
        boolean checkNull = !enableEntityMatch && enableEntityMatchGA;

        long errorLine = 0L;
        List<String> pathList = accountFileValidationServiceConfiguration.getPathList();
        String errorFile = getPath(pathList.get(0)) + PATH_SEPARATOR + ImportProperty.ERROR_FILE;
        // copy error file if file exists
        CSVFormat format = copyErrorFileToLocalIfExist(errorFile);

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
                                        id = getFieldValue(record, InterfaceName.AccountId.name());
                                    }
                                    if (StringUtils.isEmpty(id)) {
                                        log.info("Empty id is found from avro file");
                                        if (checkNull) {
                                            String lineId = getFieldValue(record, InterfaceName.InternalId.name());
                                            String message = String.format("[Required Column %s is missing value.]",
                                                    getFieldDisplayName(record, interfaceName.name(), InterfaceName.AccountId.name()));
                                            csvFilePrinter.printRecord(lineId, "", message);
                                            fileError = true;
                                            errorInPath++;
                                            errorLine++;
                                        } else {
                                            dataFileWriter.append(record);
                                        }
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
                                            errorInPath++;
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
                // modify processed records if necessary
                if (errorInPath != 0L) {
                    long processed = Long.parseLong(processedRecords.get(i)) - errorInPath;
                    processedRecords.set(i, String.valueOf(processed));
                }
            }

        } catch (IOException ex) {
            log.info("Error when writing error message to error file");
        }

        // copy error file back to hdfs, remove local error.csv
        if (errorLine != 0L) {
            copyErrorFileBackToHdfs(errorFile);
        }
        EntityValidationSummary summary = new EntityValidationSummary();
        summary.setErrorLineNumber(errorLine);
        return summary;
    }
}

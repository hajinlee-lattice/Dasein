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
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ContactFileValidationConfiguration;

@Component("contactFileValidationService")
@Lazy(value = false)
public class ContactFileValidationService
        extends InputFileValidationService<ContactFileValidationConfiguration> {


    private static final Logger log = LoggerFactory.getLogger(ContactFileValidationService.class);

    public ContactFileValidationService() {
        super(ContactFileValidationConfiguration.class.getSimpleName());
    }

    @Override
    public long validate(ContactFileValidationConfiguration contactFileValidationServiceConfiguration,
            List<String> processedRecords) {
        // first check entity match

        boolean enableEntityMatch = contactFileValidationServiceConfiguration.isEnableEntityMatch();
        boolean enableEntityMatchGA = contactFileValidationServiceConfiguration.isEnableEntityMatchGA();

        long errorLine = 0L;
        List<String> pathList = contactFileValidationServiceConfiguration.getPathList();

        CSVFormat format = LECSVFormat.format;
        // copy error file if file exists
        String errorFile = getPath(pathList.get(0)) + "/" + ImportProperty.ERROR_FILE;
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

        try (CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(ImportProperty.ERROR_FILE, true), format)) {
            for (int i = 0; i < pathList.size(); i++) {
                String path = getPath(pathList.get(i));
                long errorInPath = 0L;
                try {
                    log.info("begin dealing with path " + path);
                    List<String> avroFileList = HdfsUtils.getFilesByGlob(yarnConfiguration, path + "/*.avro");

                    for (String avroFile : avroFileList) {
                        boolean fileError = false;
                        String avrofileName = avroFile.substring(avroFile.lastIndexOf("/") + 1);
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
                                    String errorMessage = checkId(record, enableEntityMatch, enableEntityMatchGA);
                                    if (StringUtils.isNotEmpty(errorMessage)) {
                                        String lineId = getFieldValue(record, InterfaceName.InternalId.name());
                                        csvFilePrinter.printRecord(lineId, "", errorMessage);
                                        rowError = true;
                                        fileError = true;
                                        errorInPath++;
                                        errorLine++;
                                    }
                                    if (!rowError) {
                                        errorMessage = checkMatchField(record, enableEntityMatch);
                                        if (StringUtils.isNotEmpty(errorMessage)) {
                                            String lineId = getFieldValue(record, InterfaceName.InternalId.name());
                                            csvFilePrinter.printRecord(lineId, "", errorMessage);
                                            rowError = true;
                                            fileError = true;
                                            errorInPath++;
                                            errorLine++;
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
        if (errorLine != 0) {
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

    private String checkId(GenericRecord record, boolean enableEntityMatch, boolean enableEntityMatchGA) {
        if (record == null) {
            return null;
        }
        boolean checkEmpty = !enableEntityMatch && enableEntityMatchGA;
        String message = null;

        String contactIdName = (enableEntityMatch || enableEntityMatchGA) ? InterfaceName.CustomerContactId.name() :
                InterfaceName.ContactId.name();
        String accountIdName = (enableEntityMatch || enableEntityMatchGA) ? InterfaceName.CustomerAccountId.name() :
                InterfaceName.AccountId.name();
        String contactId = getFieldValue(record, contactIdName);
        String accountId = getFieldValue(record, accountIdName);
        if (checkEmpty && StringUtils.isEmpty(contactId)) {
            contactId = getFieldValue(record, InterfaceName.ContactId.name());
            if (StringUtils.isEmpty(contactId)) {
                message = "ContactId should not be empty!";
                return message;
            }
        }
        if (checkEmpty && StringUtils.isEmpty(accountId)) {
            accountId = getFieldValue(record, InterfaceName.AccountId.name());
            if (StringUtils.isEmpty(accountId)) {
                message = "AccountId should not be empty!";
                return message;
            }
        }
        if (StringUtils.isNotEmpty(contactId)) {
            String finalContactId = contactId;
            if (invalidChars.stream().anyMatch(e -> finalContactId.indexOf(e) != -1)) {
                message = String.format(
                        "Invalid character found \"/\" or \"&\" from attribute \"%s\".", finalContactId);
            }
        }
        if (StringUtils.isNotEmpty(accountId)) {
            String finalAccountId = accountId;
            if (invalidChars.stream().anyMatch(e -> finalAccountId.indexOf(e) != -1)) {
                message = StringUtils.isEmpty(message) ? String.format(
                        "Invalid character found \"/\" or \"&\" from attribute \"%s\".", finalAccountId) :
                        message + String.format("Invalid character found \"/\" or \"&\" from attribute \"%s\".", finalAccountId);
            }
        }
        return message;
    }

    private String checkMatchField(GenericRecord record, boolean enableEntityMatch) {
        if (enableEntityMatch) {
            return null;
        }
        String message = null;
        String contactId = getFieldValue(record, InterfaceName.CustomerContactId.name());
        if (StringUtils.isEmpty(contactId)) {
            contactId = getFieldValue(record, InterfaceName.ContactId.name());
        }
        String email = getFieldValue(record, InterfaceName.Email.name());
        String firstName = getFieldValue(record, InterfaceName.FirstName.name());
        String lastName = getFieldValue(record, InterfaceName.LastName.name());
        String phone = getFieldValue(record, InterfaceName.PhoneNumber.name());
        if (StringUtils.isBlank(contactId) && StringUtils.isBlank(email)
                && (StringUtils.isBlank(firstName) || StringUtils.isBlank(lastName)
                || StringUtils.isBlank(phone))) {
            message = "The contact does not have sufficient information. The contact should have should have at " +
                    "least one of the three mentioned: 1. Contact ID  2. Email 3. First name + last name + phone";
        }
        return message;
    }

}

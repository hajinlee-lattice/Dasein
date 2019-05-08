package com.latticeengines.cdl.workflow.steps.validations.service.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.MapUtils;
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
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ContactFileValidationServiceConfiguration;

@Component("contactFileValidationService")
@Lazy(value = false)
public class ContactFileValidationService
        extends InputFileValidationService<ContactFileValidationServiceConfiguration> {


    private static final Logger log = LoggerFactory.getLogger(ContactFileValidationService.class);
    public static final String serviceName = "ContactFileValidationService";

    public ContactFileValidationService() {
        super(ContactFileValidationServiceConfiguration.class.getSimpleName());
    }

    @Override
    public void validate(ContactFileValidationServiceConfiguration contactFileValidationServiceConfiguration) {
        List<String> pathList = contactFileValidationServiceConfiguration.getPathList();
        for (String path : pathList) {
            try {
                path = getPath(path);
                log.info("begin dealing with path " + path);
                List<String> matchedFiles = HdfsUtils.getFilesByGlob(yarnConfiguration, path + "/*.avro");
                for (String match : matchedFiles) {
                    String avrofileName = match.substring(match.lastIndexOf("/") + 1);
                    Map<String, String> errorMessages = new HashMap<>();
                    try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(yarnConfiguration,
                            new Path(match))) {
                        // create temp file in local
                        Schema schema = reader.getSchema();
                        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(
                                new GenericDatumWriter<>())) {
                            dataFileWriter.create(schema, new File(avrofileName));
                            // iterate through record in avro file
                            for (GenericRecord record : reader) {
                                String id = getString(record, InterfaceName.Id.name());
                                if (StringUtils.isBlank(id)) {
                                    id = getString(record, InterfaceName.ContactId.name());
                                }
                                if (StringUtils.isNotBlank(id)) {
                                    continue;
                                }
                                String email = getString(record, InterfaceName.Email.name());
                                if (StringUtils.isNotBlank(email)) {
                                    continue;
                                }
                                String firstName = getString(record, InterfaceName.FirstName.name());
                                String lastName = getString(record, InterfaceName.LastName.name());
                                String phone = getString(record, InterfaceName.PhoneNumber.name());
                                if (StringUtils.isNotBlank(firstName) && StringUtils.isNotBlank(lastName)
                                        && StringUtils.isNotBlank(phone)) {
                                    continue;
                                }
                                String lineId = getString(record, InterfaceName.InternalId.name());
                                errorMessages.put(lineId,
                                        "The contact does not have sufficient information. The contact should have should have at least one of the three mentioned: 1. Contact ID  2. Email 3. First name + last name + phone");
                                dataFileWriter.append(record);
                            }
                        }

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    // record error in error.csv if not empty, copy the
                    // new generated avro file to hdfs
                    if (MapUtils.isNotEmpty(errorMessages)) {
                        if (HdfsUtils.fileExists(yarnConfiguration, match)) {
                            HdfsUtils.rmdir(yarnConfiguration, match);
                        }
                        HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, avrofileName, match);
                        String filePath = getPath(pathList.get(0)) + "/" + ImportProperty.ERROR_FILE;
                        writeErrorFile(filePath, errorMessages);
                    }
                    FileUtils.forceDelete(new File(avrofileName));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}

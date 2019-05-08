package com.latticeengines.cdl.workflow.steps.validations.service.impl;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
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
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.AccountFileValidationServiceConfiguration;


@Component("accountFileValidationService")
@Lazy(value = false)
public class AccountFileValidationService
        extends InputFileValidationService<AccountFileValidationServiceConfiguration> {


    private static final List<Character> invalidChars = Arrays.asList('/', '&');
    private static Logger log = LoggerFactory.getLogger(AccountFileValidationService.class);

    public AccountFileValidationService() {
        super(AccountFileValidationServiceConfiguration.class.getSimpleName());
    }

    @Override
    public void validate(AccountFileValidationServiceConfiguration accountFileValidationServiceConfiguration) {

        List<String> pathList = accountFileValidationServiceConfiguration.getPathList();
        // iterate through all file, remove all illegal record row
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
                                boolean legal = true;
                                String id = getString(record, InterfaceName.Id.name());
                                if (id == null) {
                                    id = getString(record, InterfaceName.AccountId.name());
                                }
                                if (StringUtils.isEmpty(id)) {
                                    log.info("Empty id is found from avro file");
                                    continue;
                                }
                                for (Character c : invalidChars) {
                                    if (id.indexOf(c) != -1) {
                                        String lineId = getString(record, InterfaceName.InternalId.name());
                                        errorMessages.put(lineId, String.format(
                                                "Invalid account id is found due to %s in %s.", c.toString(), id));
                                        legal = false;
                                        break;
                                    }
                                }
                                if (legal) {
                                    dataFileWriter.append(record);
                                }
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

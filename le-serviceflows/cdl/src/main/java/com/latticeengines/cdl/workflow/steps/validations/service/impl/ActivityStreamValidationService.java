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
import org.apache.commons.validator.routines.UrlValidator;
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
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ActivityStreamFileValidationConfiguration;

@Component("activityStreamValidationService")
@Lazy(value = false)
public class ActivityStreamValidationService extends InputFileValidationService<ActivityStreamFileValidationConfiguration> {

    private static Logger log = LoggerFactory.getLogger(ActivityStreamValidationService.class);

    @Override
    public long validate(ActivityStreamFileValidationConfiguration inputFileValidationServiceConfiguration,
                         List<String> processedRecords, StringBuilder statistics) {
        List<String> pathList = inputFileValidationServiceConfiguration.getPathList();
        // copy error file if file exists
        String errorFile = getPath(pathList.get(0)) + PATH_SEPARATOR + ImportProperty.ERROR_FILE;
        CSVFormat format = copyErrorFileToLocalIfExist(errorFile);

        InterfaceName webVisitPageUrl = InterfaceName.WebVisitPageUrl;
        // for now support https and http, if has other requirement, need to redefine the urlValidator
        UrlValidator urlValidator = new UrlValidator(new String[] {"https", "http"});
        long errorLine = 0L;
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
                                     String url = getFieldValue(record, webVisitPageUrl.name());
                                     if (StringUtils.isNotBlank(url)) {
                                         // validate url legacy
                                         if (!urlValidator.isValid(url)) {
                                             String lineId = getFieldValue(record, InterfaceName.InternalId.name());
                                             rowError = true;
                                             fileError = true;
                                             errorInPath++;
                                             errorLine++;
                                             csvFilePrinter.printRecord(lineId, "", "invalidate url found in this row");
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

        // copy error file back to hdfs if needed, remove temporary error.csv generated in local
        if (errorLine != 0L) {
            copyErrorFileBackToHdfs(errorFile);
        }
        return errorLine;
    }
}

package com.latticeengines.cdl.workflow.steps.validations.service.impl;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.regex.Pattern;

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
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.CatalogFileValidationConfiguration;

@Component("catalogFileValidationService")
@Lazy(value = false)
public class CatalogFileValidationService extends InputFileValidationService<CatalogFileValidationConfiguration>{
    private static Logger log = LoggerFactory.getLogger(CatalogFileValidationService.class);

    private static final Pattern PATH_PATTERN = Pattern.compile("^(/[-\\w:@&?=+,.!/~*'%$_;\\(\\)]*)?$");

    @Override
    public long validate(CatalogFileValidationConfiguration catalogFileValidationServiceConfiguration,
                         List<String> processedRecords, StringBuilder statistics) {
        List<String> pathList = catalogFileValidationServiceConfiguration.getPathList();
        // copy error file if file exists
        String errorFile = getPath(pathList.get(0)) + PATH_SEPARATOR + ImportProperty.ERROR_FILE;
        CSVFormat format = copyErrorFileToLocalIfExist(errorFile);
        UrlValidator urlValidator = new UrlValidator();
        InterfaceName pathPattern = InterfaceName.PathPattern;
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
                        try (FileReader<GenericRecord> fileReader = AvroUtils.getAvroFileReader(yarnConfiguration,
                                new Path(avroFile))) {
                            Schema schema = fileReader.getSchema();
                            try (DataFileWriter<GenericRecord> dataFileWriter =
                                         new DataFileWriter<>(new GenericDatumWriter<>())) {
                                dataFileWriter.create(schema, new File(avroFileName));
                                // iterate through all records in avro files
                                for (GenericRecord record : fileReader) {
                                    boolean rowError = false;
                                    String pathStr = getFieldValue(record, pathPattern.name());
                                    if (StringUtils.isNotBlank(pathStr)) {
                                        // validate url legacy
                                        if (!urlValidator.isValid(pathStr) && !isValidPath(pathStr)) {
                                            String lineId = getFieldValue(record, InterfaceName.InternalId.name());
                                            rowError = true;
                                            fileError = true;
                                            errorInPath++;
                                            errorLine++;
                                            csvFilePrinter.printRecord(lineId, "", "invalidate path found in this row");
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

    // this part was from UrlValidator, check regex, then check if the path can generate URI and count of "//" in
    // path
    private boolean isValidPath(String path) {
        if (path == null) {
            return false;
        } else if (!PATH_PATTERN.matcher(path).matches()) {
            return false;
        } else {
            try {
                URI uri = new URI((String)null, (String)null, path, (String)null);
                String norm = uri.normalize().getPath();
                if (norm.startsWith("/../") || norm.equals("/..")) {
                    return false;
                }
            } catch (URISyntaxException var4) {
                return false;
            }

            int slash2Count = countToken("//", path);
            return slash2Count <= 0;
        }
    }

    private int countToken(String token, String target) {
        int tokenIndex = 0;
        int count = 0;

        while(tokenIndex != -1) {
            tokenIndex = target.indexOf(token, tokenIndex);
            if (tokenIndex > -1) {
                ++tokenIndex;
                ++count;
            }
        }

        return count;
    }
}

package com.latticeengines.apps.core.service.impl;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.ImportWorkflowSpecService;
import com.latticeengines.aws.s3.S3KeyFilter;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.util.ImportWorkflowSpecUtils;

@Component("importWorkflowSpecService")
public class ImportWorkflowSpecServiceImpl implements ImportWorkflowSpecService {
    private static final Logger log = LoggerFactory.getLogger(ImportWorkflowSpecServiceImpl.class);

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Value("${aws.import.specs.s3.folder}")
    private String s3Folder;

    @Value("${aws.default.access.key}")
    private String awsKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String awsSecret;

    @Inject
    private S3Service s3Service;

    public ImportWorkflowSpec loadSpecFromS3(String systemType, String systemObject) throws IOException {
        String fileSystemType = ImportWorkflowSpecUtils.sanitizeName(systemType);
        String fileSystemObject = ImportWorkflowSpecUtils.sanitizeName(systemObject);
        File specFile = null;
        try {
            specFile = File.createTempFile("temp-" + fileSystemType + "-" + fileSystemObject, ".json");
            specFile.deleteOnExit();
        } catch (IOException e) {
            throw new IOException("Could not create temp file for S3 download of spec with SystemType " + systemType +
                    " and SystemObject " + systemObject, e);
        }

        String s3Path = s3Folder + "/" + ImportWorkflowSpecUtils.constructSpecName(fileSystemType, fileSystemObject);
        log.info("Downloading file from S3 location: Bucket: " + s3Bucket + "  Key: " + s3Path);

        // Read in S3 file as InputStream.
        InputStream specInputStream = s3Service.readObjectAsStream(s3Bucket, s3Path);
        ImportWorkflowSpec workflowSpec = null;
        if (specInputStream != null) {
            try {
                workflowSpec = JsonUtils.deserialize(specInputStream, ImportWorkflowSpec.class);
            } catch (Exception e) {
                throw new IOException("JSON deserialization of Spec file from S3 bucket " + s3Bucket + " and path "
                        + s3Path + " failed", e);
            }
        } else {
            throw new IOException("Null Spec InputStream read from S3 bucket " + s3Bucket + " and path " + s3Path);
        }

        return workflowSpec;
    }

    /**
     * load spec by system type and system object, if both type and object are blank, all specs return
     * systemType and excludeSystemType are exclusive parameters
     * @param systemType
     * @param systemObject
     * @param excludeSystemType
     * @return
     */
    @Override
    public List<ImportWorkflowSpec> loadSpecsByTypeAndObject(String systemType, String systemObject,
                                                             String excludeSystemType) {
        String fileSystemType = ImportWorkflowSpecUtils.sanitizeName(systemType);
        String fileSystemObject = ImportWorkflowSpecUtils.sanitizeName(systemObject);
        String fileExcludeSystemType = ImportWorkflowSpecUtils.sanitizeName(excludeSystemType);
        boolean isBlankType = StringUtils.isBlank(fileSystemType);
        boolean isBlankExcludeType = StringUtils.isBlank(fileExcludeSystemType);
        if (!isBlankType && !isBlankExcludeType) {
            throw new IllegalArgumentException("systemType and excludeType should be exclusive params.");
        }
        log.info("Downloading file from S3 location: Bucket: " + s3Bucket + "  Key: " + s3Folder +
                " System type: " + systemType + " System object: " + systemObject);
        // Read in S3 file as InputStream.
        Iterator<InputStream> specStreamIterator = s3Service.getObjectStreamIterator(s3Bucket, s3Folder,
                new S3KeyFilter() {
                    @Override
                    public boolean accept(String key) {
                        // key example: /import-sepcs/other-contacts-spec.json
                        if (key.endsWith("/")) {
                            return false;
                        } else {
                            String name = key.substring(key.lastIndexOf("/") + 1);
                            int index = name.indexOf('-');
                            String type = name.substring(0, index);
                            String remainingPart = name.substring(index + 1);
                            if (isBlankExcludeType) {
                                return (StringUtils.isBlank(fileSystemType) || type.equals(fileSystemType))
                                        && (StringUtils.isBlank(fileSystemObject) || remainingPart.startsWith(fileSystemObject));
                            } else {
                                return (!type.equals(fileExcludeSystemType))
                                        && (StringUtils.isBlank(fileSystemObject) || remainingPart.startsWith(fileSystemObject));
                            }
                        }
                    }});
        List<ImportWorkflowSpec> specList = new ArrayList<>();
        while (specStreamIterator.hasNext()) {
            try {
                ImportWorkflowSpec workflowSpec = JsonUtils.deserialize(specStreamIterator.next(), ImportWorkflowSpec.class);
                specList.add(workflowSpec);
            } catch (Exception e) {
                log.error("JSON deserialization of Spec file from S3 bucket " + s3Bucket + " and path " + s3Folder +
                        " failed with error:", e);
                throw e;
            }
        }
        return specList;
    }

    public Table tableFromRecord(String tableName, boolean writeAllDefinitions, FieldDefinitionsRecord record) {
        log.info(String.format("Generating Table named %s from record of system type %s and object %s",
                tableName, record.getSystemObject(), record.getSystemType()));
        return ImportWorkflowSpecUtils.getTableFromFieldDefinitionsRecord(tableName, writeAllDefinitions, record);
    }

    @Override
    public void addSpecToS3(String systemType, String systemObject, ImportWorkflowSpec importWorkflowSpec) throws Exception {
        String key = s3Folder + "/" + ImportWorkflowSpecUtils.constructSpecName(systemType, systemObject);
        ByteArrayInputStream byteArrayInputStream =
                new ByteArrayInputStream(JsonUtils.pprint(importWorkflowSpec).getBytes());

        s3Service.uploadInputStream(s3Bucket, key, byteArrayInputStream, true);
    }

    @Override
    public void deleteSpecFromS3(String systemType, String systemObject) throws Exception {
        String key = s3Folder + "/" + ImportWorkflowSpecUtils.constructSpecName(systemType, systemObject);
        s3Service.cleanupDirectory(s3Bucket, key);
    }

}

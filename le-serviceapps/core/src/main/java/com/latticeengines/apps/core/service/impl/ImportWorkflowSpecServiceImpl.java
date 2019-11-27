package com.latticeengines.apps.core.service.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.ImportWorkflowSpecService;
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
        String fileSystemType = systemType.replaceAll("\\s", "").toLowerCase();
        String fileSystemObject = systemObject.replaceAll("\\s", "").toLowerCase();
        File specFile = null;
        try {
            specFile = File.createTempFile("temp-" + fileSystemType + "-" + fileSystemObject, ".json");
            specFile.deleteOnExit();
        } catch (IOException e) {
            throw new IOException("Could not create temp file for S3 download of spec with SystemType " + systemType +
                    " and SystemObject " + systemObject, e);
        }

        String s3Path = s3Folder + "/" + fileSystemType + "-" + fileSystemObject + "-spec.json";
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

    public Table tableFromRecord(String tableName, boolean writeAllDefinitions, FieldDefinitionsRecord record) {
        log.info(String.format("Generating Table named %s from record of system type %s and object %s",
                tableName, record.getSystemObject(), record.getSystemType()));
        return ImportWorkflowSpecUtils.getTableFromFieldDefinitionsRecord(tableName, writeAllDefinitions, record);
    }

}

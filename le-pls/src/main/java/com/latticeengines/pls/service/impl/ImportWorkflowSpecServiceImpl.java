package com.latticeengines.pls.service.impl;

import static com.latticeengines.pls.util.ImportWorkflowUtils.getSchemaInterpretationFromSpec;
import static com.latticeengines.pls.util.ImportWorkflowUtils.getTableFromFieldDefinitionsRecord;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.pls.service.ImportWorkflowSpecService;

@Component("importWorkflowSpecService")
public class ImportWorkflowSpecServiceImpl implements ImportWorkflowSpecService {
    private static final Logger log = LoggerFactory.getLogger(ImportWorkflowSpecServiceImpl.class);

    // TODO(jwinter): These static variables must be instance dependent and use configuration files.
    private static String s3Bucket = "latticeengines-dev";
    private static String s3Dir = "jwinter-import-workflow-testing/";

    @Inject
    private S3Service s3Service;

    @Value("${aws.default.access.key}")
    private String awsKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String awsSecret;

    public ImportWorkflowSpec loadSpecFromS3(String systemType, String systemObject) throws Exception {
        String fileSystemType = systemType.toLowerCase();
        String fileSystemObject = systemObject.toLowerCase();
        File specFile = null;
        try {
            specFile = File.createTempFile("temp-" + fileSystemType + "-" + fileSystemObject, ".json");
            specFile.deleteOnExit();
        } catch (IOException e) {
            log.error("Could not create temp file for S3 download of spec with SystemType " + systemType +
                            " and SystemObject " + systemObject);
            throw new IOException("Could not create temp file for S3 download of spec with SystemType " + systemType +
                    " and SystemObject " + systemObject + ".  Error was: " + e.getMessage());
        }

        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setBucketName(s3Bucket);
        String s3Path = s3Dir + fileSystemType + "-" + fileSystemObject + "-spec.json";
        s3ObjectSummary.setKey(s3Path);


        log.info("Downloading file from S3 location: Bucket: " + s3Bucket + "  Key: " + s3Path);

        /*
        try {
            s3Service.downloadS3File(s3ObjectSummary, specFile);
        } catch (Exception e) {
            log.error("Downloading S3 file failed with error:", e);
            throw e;
        }

        // TODO(jwinter): Consider reading the spec file into an input stream to avoid overflowing String size limits.
        String specString = null;
        if (specFile != null) {
            try {
                specString = org.apache.commons.io.FileUtils.readFileToString(specFile, Charset.defaultCharset());
                log.error("Spec is:\n" + specString);
            } catch (Exception e) {
                log.error("Reading Spec file from S3 bucket " + s3Bucket + " and path " + s3Path +
                                " into string caused exception:", e);
                throw e;
            }
        } else {
            log.error("Null Spec file read from S3 bucket " + s3Bucket + " and path " + s3Path);
            throw new IOException("Null Spec file read from S3 bucket " + s3Bucket + " and path " + s3Path);
        }
        */

        // Read in S3 file as InputStream.
        InputStream specInputStream = s3Service.readObjectAsStream(s3Bucket, s3Path);


        ImportWorkflowSpec workflowSpec = null;
        if (specInputStream == null) {
        //if (specString != null) {
            try {
                workflowSpec = JsonUtils.deserialize(specInputStream, ImportWorkflowSpec.class);
                //workflowSpec = JsonUtils.deserialize(specString, ImportWorkflowSpec.class);
                //workflowSpec.setSystemType(systemType);
                //workflowSpec.setSystemObject(systemObject);
            } catch (Exception e) {
                log.error("JSON deserialization of Spec file from S3 bucket " + s3Bucket + " and path " + s3Path +
                        " failed with error:", e);
                throw e;
            }
        } else {
            //log.error("Spec string was null when read from S3 bucket " + s3Bucket + " and path " + s3Path);
            //throw new IOException("Spec string was null when read from S3 bucket " + s3Bucket + " and path " +
            //s3Path);
            log.error("Null Spec InputStream read from S3 bucket " + s3Bucket + " and path " + s3Path);
            throw new IOException("Null Spec InputStream read from S3 bucket " + s3Bucket + " and path " + s3Path);
        }

        return workflowSpec;
    }

    public Table tableFromSpec(ImportWorkflowSpec spec) {
        Table table = getTableFromFieldDefinitionsRecord(spec, true);
        String schemaInterpretationString = getSchemaInterpretationFromSpec(spec).name();
        table.setInterpretation(schemaInterpretationString);
        // TODO(jwinter): Figure out how to better set these fields.
        table.setName(schemaInterpretationString);
        table.setDisplayName(schemaInterpretationString);

        log.info("Generating Table from Spec of type " + spec.getSystemType() + " and object " +
                spec.getSystemObject());
        return table;
    }

}

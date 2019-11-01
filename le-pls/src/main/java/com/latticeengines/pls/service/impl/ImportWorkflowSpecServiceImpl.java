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
        String fileSystemType = systemType.replaceAll("\\s", "").toLowerCase();
        String fileSystemObject = systemObject.replaceAll("\\s", "").toLowerCase();
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

        String s3Path = s3Dir + fileSystemType + "-" + fileSystemObject + "-spec.json";
        log.info("Downloading file from S3 location: Bucket: " + s3Bucket + "  Key: " + s3Path);

        // Read in S3 file as InputStream.
        InputStream specInputStream = s3Service.readObjectAsStream(s3Bucket, s3Path);
        ImportWorkflowSpec workflowSpec = null;
        if (specInputStream != null) {
            try {
                workflowSpec = JsonUtils.deserialize(specInputStream, ImportWorkflowSpec.class);
            } catch (Exception e) {
                log.error("JSON deserialization of Spec file from S3 bucket " + s3Bucket + " and path " + s3Path +
                        " failed with error:", e);
                throw e;
            }
        } else {
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

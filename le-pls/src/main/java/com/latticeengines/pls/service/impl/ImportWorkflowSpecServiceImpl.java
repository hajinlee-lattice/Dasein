package com.latticeengines.pls.service.impl;

import static com.latticeengines.pls.util.ImportWorkflowUtils.getTableFromFieldDefinitionsRecord;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.pls.service.ImportWorkflowSpecService;

@Component("importWorkflowSpecService")
public class ImportWorkflowSpecServiceImpl implements ImportWorkflowSpecService {
    private static final Logger log = LoggerFactory.getLogger(ImportWorkflowSpecServiceImpl.class);

    // TODO(jwinter): These static variables must be instance dependent and use configuration files.
    private static String s3Bucket = "latticeengines-dev";
    private static String s3Dir = "jwinter-import-workflow-testing/";

    @Inject
    private S3Service s3Service;


    public ImportWorkflowSpec loadSpecFromS3(String systemType, String systemObject) {
        systemType = systemType.toLowerCase();
        systemObject = systemObject.toLowerCase();
        File specFile = null;
        try {
            specFile = File.createTempFile("temp-" + systemType + "-" + systemObject, ".json");
            specFile.deleteOnExit();
        } catch (IOException e) {
            log.error("Could not create temp file for S3 download");
        }

        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setBucketName(s3Bucket);
        s3ObjectSummary.setKey(s3Dir + systemType + "-" + systemObject + "-spec.json");

        log.error("Downloading file from S3 location: " + s3ObjectSummary.getKey());

        try {
            s3Service.downloadS3File(s3ObjectSummary, specFile);
        } catch (Exception e) {
            log.error("Downloading S3 file failed with error:", e);
        }

        // TODO(jwinter): Consider reading the spec file into an input stream to avoid overflowing String size limits.
        String specString = null;
        if (specFile != null) {
            try {
                specString = org.apache.commons.io.FileUtils.readFileToString(specFile, Charset.defaultCharset());
                log.error("Spec is:\n" + specString);
            } catch (Exception e) {
                log.error("Reading Spec file into string caused exception:", e);
            }
        } else {
            log.error("Spec File was null");
        }

        ImportWorkflowSpec workflowSpec = null;
        if (specString != null) {
            try {
                workflowSpec = JsonUtils.deserialize(specString, ImportWorkflowSpec.class);
                workflowSpec.setSystemType(systemType);
                workflowSpec.setSystemObject(systemObject);
            } catch (Exception e) {
                log.error("JSON deserialization step failed with error:", e);
                ResponseDocument.failedResponse(e);
            }
        } else {
            log.error("===> specString was null!!!");
        }

        return workflowSpec;
    }

    public Table tableFromSpec(ImportWorkflowSpec spec) {
        Table table = getTableFromFieldDefinitionsRecord(spec, true);
        String schemaInterpretationString;
        if (spec.getSystemObject().toLowerCase().contains("account")) {
            schemaInterpretationString = SchemaInterpretation.Account.toString();
        } else if (spec.getSystemObject().toLowerCase().contains("contact") ||
                spec.getSystemObject().toLowerCase().contains("lead")) {
            schemaInterpretationString = SchemaInterpretation.Contact.toString();
        } else if (spec.getSystemObject().toLowerCase().contains("transaction")) {
            schemaInterpretationString = SchemaInterpretation.Transaction.toString();
        } else if (spec.getSystemObject().toLowerCase().contains("product")) {
            schemaInterpretationString = SchemaInterpretation.Product.toString();
        } else {
            throw new IllegalArgumentException("Spec systemObject type not yet supported");
        }

        // TODO(jwinter): Figure out how to better set these fields.
        table.setInterpretation(schemaInterpretationString);
        table.setName(schemaInterpretationString);
        table.setDisplayName(schemaInterpretationString);

        log.error("Generating Table from Spec of type " + spec.getSystemType() + " and object " +
                spec.getSystemObject());
        return table;
    }

}

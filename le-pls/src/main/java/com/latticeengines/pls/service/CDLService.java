package com.latticeengines.pls.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.frontend.TemplateFieldPreview;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;

public interface CDLService {

    ApplicationId processAnalyze(String string, ProcessAnalyzeRequest request);

    ApplicationId submitCSVImport(String customerSpace, String templateFileName,
            String dataFileName, String source, String entity, String feedType);

    String createS3Template(String customerSpace, String templateFileName, String source,
                            String entity, String feedType, String subType, String displayName);

    ApplicationId submitS3ImportWithTemplateData(String customerSpace, String taskId, String templateFileName);

    ApplicationId submitS3ImportOnlyData(String customerSpace, String taskId, String dataFileName);

    void importFileToS3(String customerSpace, String templateFileName, String s3Path);

    UIAction cleanup(String customerSpace, String sourceFileName, SchemaInterpretation schemaInterpretation,
                          CleanupOperationType cleanupOperationType);

    ApplicationId cleanupByTimeRange(String customerSpace, String startTime, String endTime,
            SchemaInterpretation schemaInterpretation);

    ApplicationId cleanupAllData(String customerSpace, SchemaInterpretation schemaInterpretation);

    List<S3ImportTemplateDisplay> getS3ImportTemplate(String string);

    List<FileProperty> getFileListForS3Path(String customerSpace, String s3Path);

    void createS3ImportSystem(String customerSpace, String systemName, S3ImportSystem.SystemType systemType);

    S3ImportSystem getS3ImportSystem(String customerSpace, String systemName);

    List<TemplateFieldPreview> getTemplatePreview(String customerSpace, Table templateTable, Table standardTable);

    boolean autoImport(String templateFileName);
}

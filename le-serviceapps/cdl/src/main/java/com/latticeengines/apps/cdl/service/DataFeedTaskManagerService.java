package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.cdl.ImportTemplateDiagnostic;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface DataFeedTaskManagerService {

    String createDataFeedTask(String customerSpaceStr, String feedType, String entity, String source, String subType,
                              String templateDisplayName, boolean sendEmail, String user, CDLImportConfig importConfig);

    String submitImportJob(String customerSpaceStr, String taskIdentifier, CDLImportConfig importConfig);

    String submitDataOnlyImportJob(String customerSpaceStr, String taskIdentifier, CSVImportConfig importConfig);

    boolean resetImport(String customerSpaceStr, BusinessEntity entity);

    String submitS3ImportJob(String customerSpaceStr, S3FileToHdfsConfiguration importConfig);

    ImportTemplateDiagnostic diagnostic(String customerSpaceStr, String taskIdentifier);
}

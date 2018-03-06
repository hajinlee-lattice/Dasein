package com.latticeengines.pls.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.db.exposed.util.MultiTenantContext;

@Component("cdlService")
public class CDLServiceImpl implements CDLService {

    private static final Logger log = LoggerFactory.getLogger(CDLServiceImpl.class);

    @Inject
    protected SourceFileService sourceFileService;

    @Inject
    private CDLProxy cdlProxy;

    @Override
    public ApplicationId processAnalyze(String customerSpace) {
        ProcessAnalyzeRequest request = new ProcessAnalyzeRequest();
        request.setUserId(MultiTenantContext.getEmailAddress());
        return cdlProxy.processAnalyze(customerSpace, request);
    }

    @Override
    public ApplicationId submitCSVImport(String customerSpace, String templateFileName, String dataFileName,
                                         String source, String entity, String feedType) {
        String email = MultiTenantContext.getEmailAddress();
        log.info(String.format("The email of the file upload initiator is %s", email));
        CSVImportConfig metaData = generateImportConfig(customerSpace.toString(), templateFileName, dataFileName,
                email);
        String taskId = cdlProxy.createDataFeedTask(customerSpace.toString(), source, entity, feedType, metaData);
        if (StringUtils.isEmpty(taskId)) {
            throw new LedpException(LedpCode.LEDP_18162, new String[] { entity, source, feedType });
        }
        return cdlProxy.submitImportJob(customerSpace.toString(), taskId, metaData);
    }

    @Override
    public ApplicationId cleanup(String customerSpace, String sourceFileName, SchemaInterpretation schemaInterpretation,
                                 CleanupOperationType cleanupOperationType) {
        BusinessEntity entity;
        switch (schemaInterpretation) {
            case DeleteAccountTemplate:
                entity = BusinessEntity.Account;
                break;
            case DeleteContactTemplate:
                entity = BusinessEntity.Contact;
                break;
            case DeleteTransactionTemplate:
                entity = BusinessEntity.Transaction;
                break;
            default:
                throw new RuntimeException("Cleanup operation does not support schema: " + schemaInterpretation.name());
        }
        SourceFile sourceFile = getSourceFile(sourceFileName);
        if (sourceFile == null) {
            throw new RuntimeException("Cannot find source file with name: " + sourceFileName);
        }
        if (StringUtils.isEmpty(sourceFile.getTableName())) {
            throw new RuntimeException(String.format("Source file %s doesn't have a schema.", sourceFileName));
        }
        String email = MultiTenantContext.getEmailAddress();
        return cdlProxy.cleanupByUpload(customerSpace, sourceFile, entity,
                cleanupOperationType, email);
    }

    @VisibleForTesting
    CSVImportConfig generateImportConfig(String customerSpace, String templateFileName, String dataFileName,
                                         String email) {
        CSVToHdfsConfiguration importConfig = new CSVToHdfsConfiguration();
        SourceFile templateSourceFile = getSourceFile(templateFileName);
        SourceFile dataSourceFile = getSourceFile(dataFileName);
        if (StringUtils.isEmpty(templateSourceFile.getTableName())) {
            throw new RuntimeException(
                    String.format("Source file %s doesn't have a table template!", templateFileName));
        }
        importConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
        importConfig.setTemplateName(templateSourceFile.getTableName());
        importConfig.setFilePath(dataSourceFile.getPath());
        importConfig.setFileSource("HDFS");
        CSVImportFileInfo importFileInfo = new CSVImportFileInfo();
        importFileInfo.setFileUploadInitiator(email);
        importFileInfo.setReportFileDisplayName(dataSourceFile.getDisplayName());
        importFileInfo.setReportFileName(dataSourceFile.getName());
        CSVImportConfig csvImportConfig = new CSVImportConfig();
        csvImportConfig.setCsvToHdfsConfiguration(importConfig);
        csvImportConfig.setCSVImportFileInfo(importFileInfo);

        return csvImportConfig;
    }

    @VisibleForTesting
    SourceFile getSourceFile(String sourceFileName) {
        SourceFile sourceFile = sourceFileService.findByName(sourceFileName);
        if (sourceFile == null) {
            throw new RuntimeException(String.format("Could not locate source file with name %s", sourceFileName));
        }
        return sourceFile;
    }
}

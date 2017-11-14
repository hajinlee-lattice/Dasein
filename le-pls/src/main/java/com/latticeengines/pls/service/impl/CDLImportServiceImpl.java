package com.latticeengines.pls.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.service.CDLImportService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

@Component("cdlImportService")
public class CDLImportServiceImpl implements CDLImportService {

    @Autowired
    protected SourceFileService sourceFileService;

    @Autowired
    private CDLProxy cdlProxy;

    @Override
    public ApplicationId submitCSVImport(String customerSpace, String templateFileName, String dataFileName,
            String source, String entity, String feedType) {
        String metaData = generateImportConfigStr(customerSpace.toString(), templateFileName, dataFileName);
        String taskId = cdlProxy.createDataFeedTask(customerSpace.toString(), source, entity, feedType, metaData);
        if (StringUtils.isEmpty(taskId)) {
            throw new LedpException(LedpCode.LEDP_18162, new String[] {entity, source, feedType});
        }
        return cdlProxy.submitImportJob(customerSpace.toString(), taskId, metaData);
    }

    @VisibleForTesting
    String generateImportConfigStr(String customerSpace, String templateFileName, String dataFileName) {
        CSVToHdfsConfiguration importConfig = new CSVToHdfsConfiguration();
        SourceFile templateSourceFile = getSourceFile(templateFileName);
        SourceFile dataSourceFile = getSourceFile(dataFileName);
        importConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
        importConfig.setTemplateName(templateSourceFile.getTableName());
        importConfig.setFilePath(dataSourceFile.getPath());
        importConfig.setFileDisplayName(dataSourceFile.getDisplayName());
        importConfig.setFileName(dataSourceFile.getName());
        importConfig.setFileSource("HDFS");

        return JsonUtils.serialize(importConfig);
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

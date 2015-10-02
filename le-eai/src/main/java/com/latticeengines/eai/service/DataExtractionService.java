package com.latticeengines.eai.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;

public interface DataExtractionService {

    void extractAndImport(ImportConfiguration importConfig, ImportContext context);

    ApplicationId submitExtractAndImportJob(ImportConfiguration importConfig);

    String createTargetPath(String customer);

    void cleanUpTargetPathData(ImportContext context) throws Exception;
}

package com.latticeengines.eai.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.metadata.Table;

public interface DataExtractionService {

    List<Table> extractAndImport(ImportConfiguration importConfig, ImportContext context);

    ApplicationId submitExtractAndImportJob(ImportConfiguration importConfig);

    String createTargetPath(String customer);

    void cleanUpTargetPathData(ImportContext context) throws Exception;
}

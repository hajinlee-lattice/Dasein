package com.latticeengines.eai.exposed.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;

public interface EaiService {

    ApplicationId extractAndImportToHdfs(ImportConfiguration importConfig);

    ApplicationId exportDataFromHdfs(ExportConfiguration exportConfig);

    ApplicationId submitEaiJob(EaiJobConfiguration eaiJobConfig);

}

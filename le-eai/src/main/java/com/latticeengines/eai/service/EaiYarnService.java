package com.latticeengines.eai.service;

import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;

public interface EaiYarnService {

    ApplicationId submitSingleYarnContainer(EaiJobConfiguration eaiJobConfig);

    ApplicationId submitMRJob(String mrJobName, Properties props);

}

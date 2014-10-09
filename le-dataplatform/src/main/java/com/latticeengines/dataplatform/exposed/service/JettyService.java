package com.latticeengines.dataplatform.exposed.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.jetty.JettyJobConfiguration;

public interface JettyService {

    ApplicationId startJettyHost(JettyJobConfiguration jettyJobConfiguration);
}

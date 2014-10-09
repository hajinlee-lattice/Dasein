package com.latticeengines.dataplatform.exposed.service.impl;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.client.yarn.AppMasterProperty;
import com.latticeengines.dataplatform.client.yarn.ContainerProperty;
import com.latticeengines.dataplatform.exposed.service.JettyService;
import com.latticeengines.dataplatform.service.jetty.JettyJobService;
import com.latticeengines.domain.exposed.jetty.JettyJob;
import com.latticeengines.domain.exposed.jetty.JettyJobConfiguration;
import com.latticeengines.scheduler.exposed.fairscheduler.LedpQueueAssigner;

@Component("jettyService")
public class JettyServiceImpl implements JettyService {

    private static final Log log = LogFactory.getLog(JettyServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private JettyJobService jettyJobService;

    @Value("${dataplatform.jetty.basedir}")
    private String jettyBaseDir;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ApplicationId startJettyHost(JettyJobConfiguration jettyJobConfiguration) {
        JettyJob jettyJob = createJob(jettyJobConfiguration);

        return jettyJobService.submitJob(jettyJob);
    }

    private JettyJob createJob(JettyJobConfiguration jettyJobConf) {
        JettyJob jettyJob = new JettyJob();
        Properties appMasterProperties = jettyJobConf.getAppMasterProps();
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getNonMRQueueNameForSubmission((int) appMasterProperties.get(AppMasterProperty.PRIORITY.name())));
        Properties containerProperties = jettyJobConf.getContainerProps();
        containerProperties.put(ContainerProperty.METADATA.name(), jettyJob.getName());
        jettyJob.setClient("jettyClient");
        jettyJob.setAppMasterPropertiesObject(appMasterProperties);
        jettyJob.setContainerPropertiesObject(containerProperties);
        return jettyJob;
    }

}

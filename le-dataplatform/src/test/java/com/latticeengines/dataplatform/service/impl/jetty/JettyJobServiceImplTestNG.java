package com.latticeengines.dataplatform.service.impl.jetty;

import static org.testng.Assert.assertNotNull;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.client.yarn.AppMasterProperty;
import com.latticeengines.dataplatform.client.yarn.ContainerProperty;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.JobNameService;
import com.latticeengines.dataplatform.service.jetty.JettyJobService;
import com.latticeengines.domain.exposed.jetty.JettyJob;

public class JettyJobServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private JettyJobService jettyJobService;

    @Autowired
    private Configuration hadoopConfiguration;

    @Autowired
    private JobNameService jobNameService;

    private String inputDir = null;
    private String outputDir = null;



    private Properties createAppMasterPropertiesForYarnJob() {
        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.PRIORITY.name(), "0");
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), "LHT");
        appMasterProperties.put(AppMasterProperty.MEMORY.name(), "128");
        appMasterProperties.put(AppMasterProperty.VIRTUALCORES.name(), "1");
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), "Priority0.0");
        appMasterProperties.put(AppMasterProperty.CONTAINER_COUNT.name(), "2");
        return appMasterProperties;
    }

    private Properties createContainerPropertiesForYarnJob() {
        Properties containerProperties = new Properties();
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "64");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");
        containerProperties.put(ContainerProperty.METADATA.name(), "helloworld");
        return containerProperties;
    }

    @Test(groups = "functional", enabled = false)
    public void testSubmit() throws Exception {
        JettyJob jettyJob = new JettyJob();
        jettyJob.setClient("jettyClient");
        jettyJob.setAppMasterPropertiesObject(createAppMasterPropertiesForYarnJob());
        jettyJob.setContainerPropertiesObject(createContainerPropertiesForYarnJob());
        jettyJob.setName("helloworld");
        ApplicationId appId = jettyJobService.submitJob(jettyJob);
        assertNotNull(appId);
    }

}

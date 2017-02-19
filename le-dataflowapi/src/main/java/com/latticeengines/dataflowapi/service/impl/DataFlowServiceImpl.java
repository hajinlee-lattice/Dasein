package com.latticeengines.dataflowapi.service.impl;

import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflowapi.service.DataFlowService;
import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.domain.exposed.dataflow.DataFlowJob;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("dataFlowService")
public class DataFlowServiceImpl implements DataFlowService {

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Autowired
    private JobService jobService;

    @Value("${dataflowapi.engine}")
    private String cascadingEngine;

    @Value("${dataflowapi.flink.mode}")
    private String flinkMode;

    @Value("${dataflowapi.flink.local.vcores}")
    private Integer flinkLocalVcores;

    @Value("${dataflowapi.flink.local.mem}")
    private Integer flinkLocalMemory;

    @Value("${dataflowapi.flink.yarn.containers}")
    private Integer flinkYarnContainers;

    @Value("${dataflowapi.flink.yarn.slots}")
    private Integer flinkYarnSlots;

    @Value("${dataflowapi.flink.yarn.tm.mem.mb}")
    private Integer flinkYarnTmMem;

    @Value("${dataflowapi.flink.yarn.jm.mem.mb}")
    private Integer flinkYarnJmMem;

    @Override
    public ApplicationId submitDataFlow(DataFlowConfiguration dataFlowConfig) {
        DataFlowJob dataFlowJob = createJob(dataFlowConfig);
        ApplicationId appId = jobService.submitJob(dataFlowJob);
        dataFlowJob.setId(appId.toString());
        jobEntityMgr.create(dataFlowJob);
        return appId;
    }

    private DataFlowJob createJob(DataFlowConfiguration dataFlowConfig) {
        DataFlowJob dataFlowJob = new DataFlowJob();

        String customer = dataFlowConfig.getCustomerSpace().toString();
        dataFlowJob.setClient("dataflowapiClient");
        dataFlowJob.setCustomer(customer);

        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), customer);
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getDataflowQueueNameForSubmission());
        appMasterProperties.put(AppMasterProperty.APP_NAME_SUFFIX.name(),
                dataFlowConfig.getDataFlowBeanName().replace(" ", "_"));

        Properties containerProperties = new Properties();
        containerProperties.put("dataflowapiConfig", dataFlowConfig.toString());
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "4096");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");

        if ("FLINK".equalsIgnoreCase(cascadingEngine) && (dataFlowConfig.getDataFlowParameters() != null
                && !dataFlowConfig.getDataFlowParameters().noFlink)) {
            if ("local".equals(flinkMode)) {
                appMasterProperties.put(AppMasterProperty.VIRTUALCORES.name(), String.valueOf(flinkLocalVcores));
                appMasterProperties.put(AppMasterProperty.MEMORY.name(), String.valueOf(flinkLocalMemory));
                containerProperties.put(ContainerProperty.VIRTUALCORES.name(), String.valueOf(flinkLocalVcores));
                containerProperties.put(ContainerProperty.MEMORY.name(), String.valueOf(flinkLocalMemory));
            }
        }

        dataFlowJob.setAppMasterPropertiesObject(appMasterProperties);
        dataFlowJob.setContainerPropertiesObject(containerProperties);
        return dataFlowJob;
    }

}

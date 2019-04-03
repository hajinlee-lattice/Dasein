package com.latticeengines.dataflowapi.service.impl;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JacocoUtils;
import com.latticeengines.dataflowapi.service.DataFlowService;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.domain.exposed.dataflow.DataFlowJob;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.client.AppMasterProperty;
import com.latticeengines.yarn.exposed.client.ContainerProperty;
import com.latticeengines.yarn.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.yarn.exposed.service.JobService;

@Component("dataFlowService")
public class DataFlowServiceImpl implements DataFlowService {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(DataFlowServiceImpl.class);

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Autowired
    private JobService jobService;

    @Value("${dataflowapi.engine}")
    private String cascadingEngine;

    @Value("${dataflowapi.flink.local.vcores}")
    private Integer flinkLocalVcores;

    @Value("${dataflowapi.flink.local.mem}")
    private Integer flinkLocalMemory;

    @Value("${dataplatform.trustore.jks}")
    private String trustStoreJks;

    @Value("${dataflowapi.am.mem}")
    private int amMemory;

    @Override
    public ApplicationId submitDataFlow(DataFlowConfiguration dataFlowConfig) {
        DataFlowJob dataFlowJob = createJob(dataFlowConfig);
        ApplicationId appId = jobService.submitJob(dataFlowJob);
        dataFlowJob.setId(appId.toString());
        dataFlowJob.setDataFlowBeanName(dataFlowConfig.getDataFlowBeanName());
        jobEntityMgr.create(dataFlowJob);
        return appId;
    }

    private DataFlowJob createJob(DataFlowConfiguration dataFlowConfig) {
        DataFlowJob dataFlowJob = new DataFlowJob();

        String customer = dataFlowConfig.getCustomerSpace().toString();
        dataFlowJob.setClient("dataflowapiClient");
        dataFlowJob.setCustomer(customer);

        int memMb = getAmMemoryMb(dataFlowConfig);
        int vcores = getAmVCores(dataFlowConfig);

        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), customer);
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getDataflowQueueNameForSubmission());
        appMasterProperties.put(AppMasterProperty.APP_NAME_SUFFIX.name(),
                dataFlowConfig.getDataFlowBeanName().replace(" ", "_"));
        appMasterProperties.put(AppMasterProperty.VIRTUALCORES.name(), String.valueOf(vcores));
        appMasterProperties.put(AppMasterProperty.MEMORY.name(), String.valueOf(memMb));

        Properties containerProperties = new Properties();
        containerProperties.put("dataflowapiConfig", dataFlowConfig.toString());
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), String.valueOf(vcores));
        containerProperties.put(ContainerProperty.MEMORY.name(), String.valueOf(memMb));
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");

        if ("FLINK".equalsIgnoreCase(cascadingEngine) && (dataFlowConfig.getDataFlowParameters() != null
                && !dataFlowConfig.getDataFlowParameters().noFlink)) {
            appMasterProperties.put(AppMasterProperty.VIRTUALCORES.name(), String.valueOf(flinkLocalVcores));
            appMasterProperties.put(AppMasterProperty.MEMORY.name(), String.valueOf(flinkLocalMemory));
            containerProperties.put(ContainerProperty.VIRTUALCORES.name(), String.valueOf(flinkLocalVcores));
            containerProperties.put(ContainerProperty.MEMORY.name(), String.valueOf(flinkLocalMemory));
        }

        String swLib = dataFlowConfig.getSwlib();
        if (StringUtils.isNotBlank(swLib)) {
            containerProperties.put(ContainerProperty.SWLIB_PKG.name(), swLib);
        }

        JacocoUtils.setJacoco(containerProperties, "dataflowapi");

        if (StringUtils.isNotBlank(trustStoreJks)) {
            containerProperties.put(ContainerProperty.TRUST_STORE.name(), trustStoreJks);
        }

        dataFlowJob.setAppMasterPropertiesObject(appMasterProperties);
        dataFlowJob.setContainerPropertiesObject(containerProperties);
        return dataFlowJob;
    }

    private int getAmMemoryMb(DataFlowConfiguration dataFlowConfig) {
        Integer memGb = dataFlowConfig.getAmMemGb();
        if (memGb == null) {
            return amMemory;
        } else {
            return memGb * 1024;
        }
    }

    private int getAmVCores(DataFlowConfiguration dataFlowConfig) {
        Integer vcores = dataFlowConfig.getAmVcores();
        if (vcores == null) {
            return 1;
        } else {
            return vcores;
        }
    }

}

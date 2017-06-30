package com.latticeengines.yarn.exposed.service.impl;

import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.yarn.exposed.client.mapreduce.MRJobCustomization;
import com.latticeengines.yarn.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.yarn.exposed.service.JobNameService;
import com.latticeengines.yarn.exposed.service.MapReduceCustomizationService;

@Component("mapReduceCustomizationService")
public class MapReduceCustomizationServiceImpl implements MapReduceCustomizationService {

    @Autowired
    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;

    @Autowired
    private JobNameService jobNameService;

    @Override
    public void addCustomizations(Job mrJob, String mrJobType, Properties properties) {
        MRJobCustomization customization = mapReduceCustomizationRegistry.getCustomization(mrJobType);
        customization.customize(mrJob, properties);

        mrJob.setJobName(jobNameService.createJobName(properties.getProperty(MapReduceProperty.CUSTOMER.name()),
                mrJob.getJobName()));
    }

    @Override
    public void validate(Job mrJob, Properties properties) {
        // TODO Auto-generated method stub

    }

}

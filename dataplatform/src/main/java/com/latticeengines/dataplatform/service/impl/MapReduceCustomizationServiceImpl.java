package com.latticeengines.dataplatform.service.impl;

import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.mapreduce.job.MRJobCustomization;
import com.latticeengines.dataplatform.mapreduce.job.MapReduceCustomizationRegistry;
import com.latticeengines.dataplatform.service.MapReduceCustomizationService;

@Component("mapReduceCustomizationService")
public class MapReduceCustomizationServiceImpl implements MapReduceCustomizationService {

    @Autowired
    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;
    
    @Override
    public void addCustomizations(Job mrJob, String mrJobType, Properties properties) {
        MRJobCustomization customization = mapReduceCustomizationRegistry.getCustomization(mrJobType);
        customization.customize(mrJob, properties);
    }

    @Override
    public void validate(Job mrJob, Properties properties) {
        // TODO Auto-generated method stub
        
    }

}

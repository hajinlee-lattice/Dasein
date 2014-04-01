package com.latticeengines.dataplatform.mapreduce.job;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.runtime.mapreduce.EventDataSamplingJob;

@Component("mapReduceCustomizationRegistry")
public class MapReduceCustomizationRegistry implements InitializingBean {

    @Autowired
    private Configuration hadoopConfiguration;

    private Map<String, MRJobCustomization> registry = new HashMap<String, MRJobCustomization>();

    public MapReduceCustomizationRegistry() {
    }

    public void register(MRJobCustomization customization) {
        registry.put(customization.getJobType(), customization);
    }

    public MRJobCustomization getCustomization(String jobType) {
        return registry.get(jobType);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        register(new EventDataSamplingJob(hadoopConfiguration));
    }

}

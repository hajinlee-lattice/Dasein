package com.latticeengines.dataplatform.exposed.client.mapreduce;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("mapReduceCustomizationRegistry")
public class MapReduceCustomizationRegistry {

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

}

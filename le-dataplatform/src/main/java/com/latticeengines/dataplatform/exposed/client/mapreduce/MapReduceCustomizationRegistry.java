package com.latticeengines.dataplatform.exposed.client.mapreduce;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

@Component("mapReduceCustomizationRegistry")
public class MapReduceCustomizationRegistry {

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

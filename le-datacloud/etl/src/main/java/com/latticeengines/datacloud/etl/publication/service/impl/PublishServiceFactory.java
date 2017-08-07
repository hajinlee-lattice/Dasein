package com.latticeengines.datacloud.etl.publication.service.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.latticeengines.datacloud.etl.publication.service.PublishService;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationConfiguration;

public class PublishServiceFactory {

    private static Map<String, PublishService> serviceMap = new ConcurrentHashMap<>();

    public static PublishService getPublishServiceBean(Class<? extends PublicationConfiguration> configClass) {
        return serviceMap.get(configClass.getSimpleName());
    }

    static void register(Class<? extends PublicationConfiguration> configClass, PublishService publishService) {
        serviceMap.put(configClass.getSimpleName(), publishService);
    }

}

package com.latticeengines.datacloud.etl.publication.service.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.latticeengines.datacloud.etl.publication.service.PublishService;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;

public class PublishServiceFactory {

    private static Map<Publication.PublicationType, PublishService> serviceMap = new ConcurrentHashMap<>();

    public static PublishService getPublishServiceBean(Publication.PublicationType publicationType) {
        return serviceMap.get(publicationType);
    }

    static void register(Publication.PublicationType publicationType, PublishService publishService) {
        serviceMap.put(publicationType, publishService);
    }

}

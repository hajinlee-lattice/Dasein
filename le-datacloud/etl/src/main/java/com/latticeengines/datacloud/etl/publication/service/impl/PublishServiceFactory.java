package com.latticeengines.datacloud.etl.publication.service.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.latticeengines.datacloud.etl.publication.service.PublishService;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;

public final class PublishServiceFactory {

    protected PublishServiceFactory() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("rawtypes")
    private static Map<Publication.PublicationType, PublishService> serviceMap = new ConcurrentHashMap<>();

    @SuppressWarnings("rawtypes")
    public static PublishService getPublishServiceBean(Publication.PublicationType publicationType) {
        return serviceMap.get(publicationType);
    }

    @SuppressWarnings("rawtypes")
    static void register(Publication.PublicationType publicationType, PublishService publishService) {
        serviceMap.put(publicationType, publishService);
    }

}

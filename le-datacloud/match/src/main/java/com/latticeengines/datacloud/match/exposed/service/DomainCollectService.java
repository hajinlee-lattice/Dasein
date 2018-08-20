package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.datacloud.collection.service.CollectionDBService;

public interface DomainCollectService {

    void enqueue(String domain);

    void dumpQueue();

    int getQueueSize();

    void setDrainMode();

    void setCollectionService(CollectionDBService service);
}

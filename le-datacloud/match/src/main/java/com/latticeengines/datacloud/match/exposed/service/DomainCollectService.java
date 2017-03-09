package com.latticeengines.datacloud.match.exposed.service;

public interface DomainCollectService {

    void enqueue(String domain);

    void dumpQueue();

}

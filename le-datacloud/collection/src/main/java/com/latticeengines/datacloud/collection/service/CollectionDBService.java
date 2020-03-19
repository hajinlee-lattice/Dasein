package com.latticeengines.datacloud.collection.service;

import java.sql.Timestamp;
import java.util.List;


public interface CollectionDBService {
    //int transferRawRequests(boolean deleteFilteredReqs);
    //int spawnCollectionWorker() throws Exception;
    //int updateCollectingStatus() throws Exception;

    boolean addNewDomains(List<String> domains, String vendor, String reqId);

    void addNewDomains(List<String> domains, String reqId);

    int getActiveTaskCount();

    boolean collect(boolean forceCollect);

    int getIngestionTaskCount();

    void ingest();

    void consolidate(List<String> effectiveVendors);

    void cleanup(Timestamp start, Timestamp end);
}

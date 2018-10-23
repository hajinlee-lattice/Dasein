package com.latticeengines.datacloud.collection.service;

import java.util.List;

public interface VendorConfigService {

    String getDomainField(String vendor);

    String getDomainCheckField(String vendor);

    int getMaxActiveTasks(String vendor);

    long getCollectingFreq(String vendor);

    List<String> getVendors();

    List<String> getEnabledVendors();

    int getDefCollectionBatch();

    int getDefMaxRetries();

}

package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.serviceapps.cdl.DataCollectionPrechecks;

public interface DataCollectionPrecheckService {
    DataCollectionPrechecks validateDataCollectionPrechecks(String customerSpace);
}

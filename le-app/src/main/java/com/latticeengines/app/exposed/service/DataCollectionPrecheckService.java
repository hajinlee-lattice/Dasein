package com.latticeengines.app.exposed.service;

import com.latticeengines.domain.exposed.serviceapps.cdl.DataCollectionPrechecks;

public interface DataCollectionPrecheckService {
    DataCollectionPrechecks validateDataCollectionPrechecks(String customerSpace);
}
